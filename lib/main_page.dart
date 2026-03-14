import 'dart:async';
import 'dart:ui' as ui;
import 'package:flutter/material.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart';
import 'connection_service.dart';

// ── Sensor palette ────────────────────────────────────────────────────────────
const List<Color> kSensorColors = [
  Color(0xFF4FC3F7), // S1 — sky blue
  Color(0xFFEF5350), // S2 — red
  Color(0xFF66BB6A), // S3 — green
  Color(0xFFFFCA28), // S4 — amber
  Color(0xFFCE93D8), // S5 — purple
  Color(0xFFFF8A65), // S6 — orange
];

// ─────────────────────────────────────────────────────────────────────────────

class MainPage extends StatelessWidget {
  final ConnectionService bleService;
  const MainPage({super.key, required this.bleService});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFF0D1117),
      appBar: AppBar(
        backgroundColor: const Color(0xFF161B22),
        title: const Text(
          'Apex Monitor',
          style: TextStyle(color: Colors.white, fontWeight: FontWeight.w500, fontSize: 18),
        ),
        actions: [
          _ConnectionBadge(bleService: bleService),
          const SizedBox(width: 8),
        ],
      ),
      body: Column(
        children: [
          // ── Graph ────────────────────────────────────────────────────────
          Expanded(
            child: Padding(
              padding: const EdgeInsets.fromLTRB(8, 12, 8, 4),
              child: SensorGraph(packetStream: bleService.packetStream),
            ),
          ),
          // ── Legend ───────────────────────────────────────────────────────
          _SensorLegend(statusStream: bleService.statusStream),
          const SizedBox(height: 12),
        ],
      ),
    );
  }
}

// ── Connection badge (AppBar top-right) ───────────────────────────────────────

class _ConnectionBadge extends StatelessWidget {
  final ConnectionService bleService;
  const _ConnectionBadge({required this.bleService});

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<BluetoothConnectionState>(
      stream: bleService.statusStream,
      builder: (context, stateSnap) {
        final isConnected =
            stateSnap.data == BluetoothConnectionState.connected;

        return StreamBuilder<bool>(
          stream: bleService.scanningStream,
          initialData: true,
          builder: (context, scanSnap) {
            final isBusy = scanSnap.data ?? false;

            if (isConnected) {
              return _Badge(
                color: const Color(0xFF238636),
                icon: Icons.bluetooth_connected,
                label: 'Connected',
              );
            }

            if (isBusy) {
              return Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  SizedBox(
                    width: 14,
                    height: 14,
                    child: CircularProgressIndicator(
                      strokeWidth: 1.5,
                      color: Colors.orange.shade300,
                    ),
                  ),
                  const SizedBox(width: 6),
                  Text(
                    'Scanning…',
                    style: TextStyle(fontSize: 12, color: Colors.orange.shade300),
                  ),
                ],
              );
            }

            // Disconnected + idle → show reconnect button
            return TextButton.icon(
              onPressed: () async {
                try {
                  await bleService.performManualDiagnostic();
                } catch (_) {
                  if (context.mounted) {
                    ScaffoldMessenger.of(context).showSnackBar(
                      const SnackBar(content: Text('Bluetooth not ready')),
                    );
                  }
                }
              },
              icon: const Icon(Icons.refresh, size: 14, color: Colors.orange),
              label: Text(
                'Reconnect',
                style: TextStyle(fontSize: 12, color: Colors.orange.shade300),
              ),
              style: TextButton.styleFrom(
                padding: const EdgeInsets.symmetric(horizontal: 8),
                minimumSize: Size.zero,
                tapTargetSize: MaterialTapTargetSize.shrinkWrap,
              ),
            );
          },
        );
      },
    );
  }
}

class _Badge extends StatelessWidget {
  final Color color;
  final IconData icon;
  final String label;
  const _Badge({required this.color, required this.icon, required this.label});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 5),
      decoration: BoxDecoration(
        color: color.withValues(alpha: 0.15),
        borderRadius: BorderRadius.circular(20),
        border: Border.all(color: color.withValues(alpha: 0.4), width: 0.8),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 13, color: color),
          const SizedBox(width: 5),
          Text(label, style: TextStyle(fontSize: 12, color: color, fontWeight: FontWeight.w500)),
        ],
      ),
    );
  }
}

// ── Sensor legend row ─────────────────────────────────────────────────────────

class _SensorLegend extends StatelessWidget {
  final Stream<BluetoothConnectionState> statusStream;
  const _SensorLegend({required this.statusStream});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceEvenly,
        children: List.generate(6, (i) {
          return Row(
            mainAxisSize: MainAxisSize.min,
            children: [
              Container(
                width: 20,
                height: 3,
                decoration: BoxDecoration(
                  color: kSensorColors[i],
                  borderRadius: BorderRadius.circular(2),
                ),
              ),
              const SizedBox(width: 4),
              Text(
                'S${i + 1}',
                style: TextStyle(
                  color: kSensorColors[i].withValues(alpha: 0.85),
                  fontSize: 12,
                  fontWeight: FontWeight.w500,
                ),
              ),
            ],
          );
        }),
      ),
    );
  }
}

// ── Real-time sensor graph ────────────────────────────────────────────────────

class _DataPoint {
  final double timeMs;
  final double value; // raw ADC 0–4095
  const _DataPoint(this.timeMs, this.value);
}

class SensorGraph extends StatefulWidget {
  final Stream<ImpactPacket> packetStream;
  const SensorGraph({super.key, required this.packetStream});

  @override
  State<SensorGraph> createState() => _SensorGraphState();
}

class _SensorGraphState extends State<SensorGraph> {
  static const int _numSensors = 6;
  static const double _windowMs = 12000; // 12-second rolling window
  static const double _maxAdcValue = 4095;
  // Gap between two consecutive samples that indicates a segment boundary.
  // Within a packet: 1 ms apart. Between packets: ≥200 ms (lockout).
  static const double _segmentGapMs = 50;

  // One bucket per sensor, storing all points within the current window
  final List<List<_DataPoint>> _data = List.generate(_numSensors, (_) => []);

  StreamSubscription<ImpactPacket>? _sub;
  Timer? _ticker;
  double _nowMs = 0;

  @override
  void initState() {
    super.initState();
    _nowMs = DateTime.now().millisecondsSinceEpoch.toDouble();

    _sub = widget.packetStream.listen(_onPacket);

    // Scroll the window at ~30 fps — no sensor data needed to advance time
    _ticker = Timer.periodic(const Duration(milliseconds: 33), (_) {
      setState(() {
        _nowMs = DateTime.now().millisecondsSinceEpoch.toDouble();
        _pruneOldData();
      });
    });
  }

  void _onPacket(ImpactPacket pkt) {
    final idx = pkt.sensorId - 1;
    if (idx < 0 || idx >= _numSensors) return;
    if (pkt.sampleIntervalUs == 0) return;

    final receiveMs = DateTime.now().millisecondsSinceEpoch.toDouble();
    final intervalMs = pkt.sampleIntervalUs / 1000.0;

    // Place every sample in absolute app time.
    // The last sample in the packet is placed at receiveMs;
    // earlier samples are offset backwards by their position × intervalMs.
    for (int i = 0; i < pkt.samples.length; i++) {
      final offsetMs = (pkt.samples.length - 1 - i) * intervalMs;
      _data[idx].add(_DataPoint(receiveMs - offsetMs, pkt.samples[i].toDouble()));
    }

    // Keep list ordered (packets from the same sensor should already be ordered,
    // but sort defensively so the painter can rely on it)
    _data[idx].sort((a, b) => a.timeMs.compareTo(b.timeMs));
  }

  void _pruneOldData() {
    // Discard anything older than the visible window plus a small buffer
    final cutoff = _nowMs - _windowMs - 500;
    for (final points in _data) {
      points.removeWhere((p) => p.timeMs < cutoff);
    }
  }

  @override
  void dispose() {
    _sub?.cancel();
    _ticker?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return ClipRRect(
      borderRadius: BorderRadius.circular(12),
      child: CustomPaint(
        painter: _GraphPainter(
          data: _data,
          nowMs: _nowMs,
          windowMs: _windowMs,
          maxValue: _maxAdcValue,
          segmentGapMs: _segmentGapMs,
        ),
        child: const SizedBox.expand(),
      ),
    );
  }
}

// ── Painter ───────────────────────────────────────────────────────────────────

class _GraphPainter extends CustomPainter {
  final List<List<_DataPoint>> data;
  final double nowMs;
  final double windowMs;
  final double maxValue;
  final double segmentGapMs;

  // Layout constants
  static const double _padLeft = 38;
  static const double _padRight = 8;
  static const double _padTop = 10;
  static const double _padBottom = 22;

  _GraphPainter({
    required this.data,
    required this.nowMs,
    required this.windowMs,
    required this.maxValue,
    required this.segmentGapMs,
  });

  // ── Coordinate helpers ───────────────────────────────────────────────────

  double _toX(double timeMs, double plotW) {
    return _padLeft + (timeMs - (nowMs - windowMs)) / windowMs * plotW;
  }

  double _toY(double value, double plotH) {
    return _padTop + plotH * (1.0 - (value / maxValue).clamp(0.0, 1.0));
  }

  // ── Paint ────────────────────────────────────────────────────────────────

  @override
  void paint(Canvas canvas, Size size) {
    final plotW = size.width - _padLeft - _padRight;
    final plotH = size.height - _padTop - _padBottom;

    // Background
    canvas.drawRect(
      Rect.fromLTWH(0, 0, size.width, size.height),
      Paint()..color = const Color(0xFF0D1117),
    );

    // Plot area background (slightly lighter)
    canvas.drawRRect(
      RRect.fromRectAndRadius(
        Rect.fromLTWH(_padLeft, _padTop, plotW, plotH),
        const Radius.circular(4),
      ),
      Paint()..color = const Color(0xFF161B22),
    );

    _drawGrid(canvas, plotW, plotH);

    // Clip traces to plot area before drawing
    canvas.save();
    canvas.clipRect(Rect.fromLTWH(_padLeft, _padTop, plotW, plotH));
    _drawTraces(canvas, plotW, plotH);
    canvas.restore();

    _drawAxesLabels(canvas, plotW, plotH, size);
  }

  // ── Grid ─────────────────────────────────────────────────────────────────

  void _drawGrid(Canvas canvas, double plotW, double plotH) {
    final gridPaint = Paint()
      ..color = const Color(0xFF21262D)
      ..strokeWidth = 0.5;

    const hLines = 5;
    for (int i = 0; i <= hLines; i++) {
      final y = _padTop + plotH * i / hLines;
      canvas.drawLine(Offset(_padLeft, y), Offset(_padLeft + plotW, y), gridPaint);
    }

    const vLines = 6; // one per 2 seconds in a 12s window
    for (int i = 0; i <= vLines; i++) {
      final x = _padLeft + plotW * i / vLines;
      canvas.drawLine(Offset(x, _padTop), Offset(x, _padTop + plotH), gridPaint);
    }
  }

  // ── Axis labels ──────────────────────────────────────────────────────────

  void _drawAxesLabels(Canvas canvas, double plotW, double plotH, Size size) {
    // Y axis — ADC values: 0, 1k, 2k, 3k, 4k
    const hLines = 5;
    for (int i = 0; i <= hLines; i++) {
      final y = _padTop + plotH * i / hLines;
      final adcValue = maxValue * (hLines - i) / hLines;
      final label = adcValue >= 1000
          ? '${(adcValue / 1000).toStringAsFixed(0)}k'
          : adcValue.toStringAsFixed(0);
      _drawLabel(
        canvas,
        label,
        Offset(_padLeft - 4, y),
        align: ui.TextAlign.right,
      );
    }

    // X axis — relative time
    const vLines = 6;
    for (int i = 0; i <= vLines; i++) {
      final x = _padLeft + plotW * i / vLines;
      final secsAgo = windowMs / 1000 * (vLines - i) / vLines;
      final label = secsAgo == 0 ? 'now' : '-${secsAgo.toStringAsFixed(0)}s';
      _drawLabel(
        canvas,
        label,
        Offset(x, _padTop + plotH + 4),
        align: ui.TextAlign.center,
      );
    }
  }

  void _drawLabel(
    Canvas canvas,
    String text,
    Offset position, {
    ui.TextAlign align = ui.TextAlign.left,
  }) {
    final tp = TextPainter(
      text: TextSpan(
        text: text,
        style: const TextStyle(
          color: Color(0xFF8B949E),
          fontSize: 10,
          fontFamily: 'monospace',
        ),
      ),
      textDirection: ui.TextDirection.ltr,
      textAlign: align,
    )..layout();

    double dx = position.dx;
    if (align == ui.TextAlign.right) dx -= tp.width;
    if (align == ui.TextAlign.center) dx -= tp.width / 2;

    tp.paint(canvas, Offset(dx, position.dy - tp.height / 2));
  }

  // ── Traces ───────────────────────────────────────────────────────────────

  void _drawTraces(Canvas canvas, double plotW, double plotH) {
    final startMs = nowMs - windowMs;

    for (int s = 0; s < data.length; s++) {
      final points = data[s];
      if (points.isEmpty) continue;

      final paint = Paint()
        ..color = kSensorColors[s]
        ..strokeWidth = 1.6
        ..style = PaintingStyle.stroke
        ..strokeCap = StrokeCap.round
        ..strokeJoin = StrokeJoin.round;

      final path = Path();
      bool pathStarted = false;
      double? prevTimeMs;

      for (final pt in points) {
        if (pt.timeMs < startMs) continue;

        final x = _toX(pt.timeMs, plotW);
        final y = _toY(pt.value, plotH);

        // Start a new sub-path when there's a gap (between separate impact events)
        final isNewSegment =
            !pathStarted || (prevTimeMs != null && pt.timeMs - prevTimeMs > segmentGapMs);

        if (isNewSegment) {
          path.moveTo(x, y);
          pathStarted = true;
        } else {
          path.lineTo(x, y);
        }

        prevTimeMs = pt.timeMs;
      }

      canvas.drawPath(path, paint);
    }
  }

  @override
  bool shouldRepaint(_GraphPainter old) => true;
}
