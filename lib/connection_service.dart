import 'dart:async';
import 'dart:io';
import 'package:flutter/material.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart';
import 'package:permission_handler/permission_handler.dart';

// ── Data model ────────────────────────────────────────────────────────────────
// Matches the binary packet produced by BLEService.ino:
//
//  Offset  Size  Field
//  0       1     sensorId         (uint8,  1-based)
//  1       2     eventId          (uint16 LE)
//  3       4     triggerUs        (uint32 LE, Arduino micros())
//  7       2     sampleIntervalUs (uint16 LE)
//  9       1     sampleCount      (uint8)
//  10      N×2   samples[]        (uint16[] LE, 12-bit ADC, 0–4095)
class ImpactPacket {
  final int sensorId;
  final int eventId;
  final int triggerUs;
  final int sampleIntervalUs;
  final List<int> samples; // raw 12-bit ADC values

  const ImpactPacket({
    required this.sensorId,
    required this.eventId,
    required this.triggerUs,
    required this.sampleIntervalUs,
    required this.samples,
  });
}

// ─────────────────────────────────────────────────────────────────────────────

class ConnectionService {
  static const String deviceName = "XIAO_S3_Piezo";
  static final Guid serviceUuid = Guid("4fafc201-1fb5-459e-8fcc-c5c9c331914b");
  static final Guid charUuid = Guid("beb5483e-36e1-4688-b7f5-ea07361b26a8");

  bool _isManualCooldown = false;
  bool _isCurrentlyScanning = false;

  // New stream to tell the UI if we are in a "processing" state
  final _scanningController = StreamController<bool>.broadcast();
  Stream<bool> get scanningStream => _scanningController.stream;

  late BluetoothDevice _targetDevice;
  StreamSubscription<BluetoothConnectionState>? _stateSubscription;

  // Emits structured binary packets instead of raw strings
  final _packetController = StreamController<ImpactPacket>.broadcast();
  Stream<ImpactPacket> get packetStream => _packetController.stream;

  final _statusController = StreamController<BluetoothConnectionState>.broadcast();
  Stream<BluetoothConnectionState> get statusStream => _statusController.stream;

  // ── Connection logic — NOT MODIFIED ─────────────────────────────────────────

  Future<void> start() async {
    _triggerCooldown();
    bool hasPermission = await checkHardwareAndPermissions();
    if (hasPermission) {
      _startScan();
    } else {
      _statusController.add(BluetoothConnectionState.disconnected);
    }
  }

  void _triggerCooldown() {
    _isManualCooldown = true;
    _scanningController.add(true);
    Timer(const Duration(seconds: 10), () {
      _isManualCooldown = false;
      _scanningController.add(_isCurrentlyScanning);
    });
  }

  Future<void> performManualDiagnostic() async {
    if (_isCurrentlyScanning || _isManualCooldown) return;
    _triggerCooldown();

    bool ready = await checkHardwareAndPermissions();
    if (!ready) {
      throw Exception("Hardware or Permissions not ready");
    }

    await FlutterBluePlus.stopScan();
    _startScan();
  }

  Future<bool> checkHardwareAndPermissions() async {
    if (await FlutterBluePlus.adapterState.first != BluetoothAdapterState.on) {
      return false;
    }

    if (Platform.isAndroid) {
      var status = await [
        Permission.bluetoothScan,
        Permission.bluetoothConnect,
        Permission.location,
      ].request();
      if (status.values.any((s) => !s.isGranted)) return false;
    }
    return true;
  }

  void _startScan() async {
    if (FlutterBluePlus.isScanningNow) return;

    _isCurrentlyScanning = true;
    _scanningController.add(true);

    StreamSubscription<bool>? scanSubscription;
    scanSubscription = FlutterBluePlus.isScanning.listen((scanning) {
      if (!scanning) {
        _isCurrentlyScanning = false;
        if (!_isManualCooldown) {
          _scanningController.add(false);
        }
        scanSubscription?.cancel();
      }
    });

    await FlutterBluePlus.startScan(
      timeout: const Duration(seconds: 10),
      androidUsesFineLocation: true,
    );

    FlutterBluePlus.scanResults.listen((results) {
      for (ScanResult r in results) {
        if (r.device.platformName == deviceName) {
          FlutterBluePlus.stopScan();
          _connect(r.device);
          break;
        }
      }
    });
  }

  void _connect(BluetoothDevice device) async {
    _targetDevice = device;

    _stateSubscription?.cancel();
    _stateSubscription = device.connectionState.listen((state) {
      _statusController.add(state);
      if (state == BluetoothConnectionState.disconnected) {
        _startScan();
      }
    });

    try {
      await _targetDevice.connect(license: License.free);
      var services = await _targetDevice.discoverServices();
      for (var s in services) {
        if (s.uuid == serviceUuid) {
          for (var c in s.characteristics) {
            if (c.uuid == charUuid) {
              await c.setNotifyValue(true);
              // Parse binary packets instead of decoding as UTF-8 strings
              c.onValueReceived.listen((value) {
                final packet = _parseBinaryPacket(value);
                if (packet != null) _packetController.add(packet);
              });
            }
          }
        }
      }
    } catch (e) {
      debugPrint("Connection error: $e");
    }
  }

  // ── Binary parser ────────────────────────────────────────────────────────────

  ImpactPacket? _parseBinaryPacket(List<int> value) {
    if (value.length < 10) return null;

    final sensorId = value[0];
    final eventId = value[1] | (value[2] << 8);
    final triggerUs = value[3] | (value[4] << 8) | (value[5] << 16) | (value[6] << 24);
    final sampleIntervalUs = value[7] | (value[8] << 8);
    final sampleCount = value[9];

    if (value.length < 10 + sampleCount * 2) return null;
    if (sensorId < 1 || sensorId > 6) return null;

    final samples = <int>[];
    for (int i = 0; i < sampleCount; i++) {
      final offset = 10 + i * 2;
      samples.add(value[offset] | (value[offset + 1] << 8));
    }

    return ImpactPacket(
      sensorId: sensorId,
      eventId: eventId,
      triggerUs: triggerUs,
      sampleIntervalUs: sampleIntervalUs,
      samples: samples,
    );
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────────

  void dispose() {
    _stateSubscription?.cancel();
    _scanningController.close();
    _packetController.close();
    _statusController.close();
  }
}
