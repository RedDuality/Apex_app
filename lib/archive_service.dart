import 'dart:async';
import 'dart:io';
import 'package:flutter/foundation.dart';
import 'package:path_provider/path_provider.dart';
import 'connection_service.dart';

// ── ArchiveService ─────────────────────────────────────────────────────────────
//
// Strategy: stream directly to disk via an IOSink opened in append mode.
// Nothing is kept in RAM beyond a small write-buffer (_flushEvery rows) that
// is handed to the OS on every flush. A 3-hour session at 6 sensors × 40
// samples × ~5 hits/min produces roughly 10 MB — trivial on disk, but the old
// approach would have held hundreds of thousands of _Row objects in the heap.
//
// Crash safety: worst case you lose the last _flushEvery rows (~1 second of
// data). The header and all previously flushed rows are already on disk.
//
// File layout (one file per session):
//   apex_YYYY-MM-DD_HH-mm-ss.csv   ← named after recording start time
//
// CSV columns:
//   received_iso        — wall-clock ISO-8601 when this packet arrived on the app
//   event_id            — shared across sensors triggered within the same impact
//   sensor_id           — 1-based sensor number (1–6)
//   trigger_us          — Arduino micros() at first threshold crossing (use for triangulation)
//   sample_index        — position of this sample within its waveform burst (0-based)
//   sample_offset_ms    — sample_index × sampleIntervalUs / 1000
//   adc_raw             — 12-bit ADC reading (0–4095)
//   voltage_v           — true voltage after 1MΩ / 47kΩ divider
// ─────────────────────────────────────────────────────────────────────────────

class ArchiveService {
  // ── Voltage divider (must match BLEService.ino) ───────────────────────────
  static const double _r1 = 1000000.0;
  static const double _r2 = 47000.0;
  static const double _dividerRatio = (_r1 + _r2) / _r2;
  static const double _adcMax = 4095.0;
  static const double _vRef = 3.3;

  // Flush every N rows. 120 rows = 3 packets (40 samples each) = roughly 1 s
  // of data. Keeps write-syscall frequency low while bounding RAM to a handful
  // of strings at any one time.
  static const int _flushEvery = 120;

  final Stream<ImpactPacket> _source;

  ArchiveService(this._source);

  // ── Public state ──────────────────────────────────────────────────────────

  /// True while actively recording.
  final ValueNotifier<bool> isRecording = ValueNotifier(false);

  /// Running count of packets written this session (drives the UI counter).
  final ValueNotifier<int> packetCount = ValueNotifier(0);

  // ── Private state ─────────────────────────────────────────────────────────

  IOSink? _sink;
  File? _currentFile;
  int _rowsSinceFlush = 0;

  StreamSubscription<ImpactPacket>? _sub;

  // ── Control ───────────────────────────────────────────────────────────────

  /// Open the CSV file and start writing. Safe to call when already recording
  /// (subsequent calls are no-ops).
  Future<void> startRecording() async {
    if (isRecording.value) return;

    final startTime = DateTime.now();
    final dir = await _resolveDirectory();
    final fileName = 'apex_${_formatFileStamp(startTime)}.csv';
    _currentFile = File('${dir.path}/$fileName');

    _sink = _currentFile!.openWrite(mode: FileMode.writeOnly);

    // Write the CSV header immediately — the file is valid even if 0 rows follow.
    _sink!.writeln(
      'received_iso,event_id,sensor_id,trigger_us,'
      'sample_index,sample_offset_ms,adc_raw,voltage_v',
    );
    await _sink!.flush();

    _rowsSinceFlush = 0;
    packetCount.value = 0;

    _sub = _source.listen(_onPacket, onError: _onStreamError);
    isRecording.value = true;
  }

  /// Flush, close, and return the saved file path.
  Future<String> stopRecording() async {
    if (!isRecording.value) throw StateError('Not currently recording');

    // Stop receiving data before closing so no writes race the sink close.
    await _sub?.cancel();
    _sub = null;

    // flush() drains the IOSink internal buffer to the OS.
    // close() waits for the OS buffer to reach the storage device.
    await _sink?.flush();
    await _sink?.close();
    _sink = null;

    isRecording.value = false;

    final path = _currentFile!.path;
    _currentFile = null;
    return path;
  }

  // ── Stream handler ────────────────────────────────────────────────────────

  void _onPacket(ImpactPacket pkt) {
    final sink = _sink;
    if (sink == null || pkt.sampleIntervalUs == 0) return;

    final receivedIso = DateTime.now().toIso8601String();
    final intervalMs = pkt.sampleIntervalUs / 1000.0;

    for (int i = 0; i < pkt.samples.length; i++) {
      final adcRaw = pkt.samples[i];
      final pinV = (adcRaw / _adcMax) * _vRef;
      final trueV = pinV * _dividerRatio;

      // writeln() on an IOSink appends to an internal byte buffer — no syscall.
      // The flush() below is the only OS write interaction.
      sink.writeln(
        '$receivedIso,${pkt.eventId},${pkt.sensorId},${pkt.triggerUs},'
        '$i,${(i * intervalMs).toStringAsFixed(3)},'
        '$adcRaw,${trueV.toStringAsFixed(4)}',
      );
      _rowsSinceFlush++;
    }

    packetCount.value++;

    // Periodic flush. We don't await the returned Future so the packet
    // callback stays synchronous and never blocks the BLE receive loop.
    if (_rowsSinceFlush >= _flushEvery) {
      sink.flush();
      _rowsSinceFlush = 0;
    }
  }

  void _onStreamError(Object error) {
    debugPrint('ArchiveService stream error: $error');
  }

  // ── Directory resolution ──────────────────────────────────────────────────

  Future<Directory> _resolveDirectory() async {
    // Android: prefer external storage (visible in the file manager and over USB).
    if (Platform.isAndroid) {
      try {
        final ext = await getExternalStorageDirectory();
        if (ext != null) return ext;
      } catch (_) {}
    }
    // iOS / fallback.
    return getApplicationDocumentsDirectory();
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  String _formatFileStamp(DateTime dt) {
    String p(int n, [int w = 2]) => n.toString().padLeft(w, '0');
    return '${p(dt.year, 4)}-${p(dt.month)}-${p(dt.day)}'
        '_${p(dt.hour)}-${p(dt.minute)}-${p(dt.second)}';
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  Future<void> dispose() async {
    await _sub?.cancel();
    await _sink?.flush();
    await _sink?.close();
    isRecording.dispose();
    packetCount.dispose();
  }
}
