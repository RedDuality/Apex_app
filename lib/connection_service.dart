import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:flutter/material.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart';
import 'package:permission_handler/permission_handler.dart';

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

  final _dataController = StreamController<String>.broadcast();
  Stream<String> get dataStream => _dataController.stream;

  final _statusController = StreamController<BluetoothConnectionState>.broadcast();
  Stream<BluetoothConnectionState> get statusStream => _statusController.stream;

  Future<void> start() async {
    _triggerCooldown();
    bool hasPermission = await checkHardwareAndPermissions();
    if (hasPermission) {
      _startScan();
    } else {
      _statusController.add(BluetoothConnectionState.disconnected);
    }
  }

  // Logic to handle the 10-second button hide/disable
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

    // Re-verify hardware/permissions first
    bool ready = await checkHardwareAndPermissions();
    if (!ready) {
      throw Exception("Hardware or Permissions not ready");
    }

    // Force stop any existing scan and start fresh
    await FlutterBluePlus.stopScan();
    _startScan();
  }

  Future<bool> checkHardwareAndPermissions() async {
    // Check if Bluetooth is actually ON
    if (await FlutterBluePlus.adapterState.first != BluetoothAdapterState.on) {
      return false;
    }

    // Check Android-specific permissions
    if (Platform.isAndroid) {
      var status = await [Permission.bluetoothScan, Permission.bluetoothConnect, Permission.location].request();
      if (status.values.any((s) => !s.isGranted)) return false;
    }
    return true;
  }

  void _startScan() async {
    if (FlutterBluePlus.isScanningNow) return;

    _isCurrentlyScanning = true;
    _scanningController.add(true);

    // 1. Listen for the hardware scan to stop (either timeout or manual stop)
    StreamSubscription<bool>? scanSubscription;
    scanSubscription = FlutterBluePlus.isScanning.listen((scanning) {
      if (!scanning) {
        _isCurrentlyScanning = false;
        // Only update UI if we aren't still in the 10s "manual cooldown" window
        if (!_isManualCooldown) {
          _scanningController.add(false);
        }
        scanSubscription?.cancel();
      }
    });

    // 2. Start the scan with a timeout
    await FlutterBluePlus.startScan(
      timeout: const Duration(seconds: 10),
      androidUsesFineLocation: true, // Recommended for Android 12+
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
              c.onValueReceived.listen((value) => _dataController.add(utf8.decode(value)));
            }
          }
        }
      }
    } catch (e) {
      debugPrint("Connection error: $e");
    }
  }

  void dispose() {
    _stateSubscription?.cancel();
    _scanningController.close();
    _dataController.close();
    _statusController.close();
  }
}
