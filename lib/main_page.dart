import 'package:flutter/material.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart';
import 'connection_service.dart';

class MainPage extends StatelessWidget {
  final ConnectionService bleService;
  const MainPage({super.key, required this.bleService});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text("Apex Monitor")),
      body: StreamBuilder<BluetoothConnectionState>(
        stream: bleService.statusStream,
        builder: (context, snapshot) {
          final state = snapshot.data ?? BluetoothConnectionState.disconnected;
          final bool isConnected = state == BluetoothConnectionState.connected;

          return Center(
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                // 1. Connection Indicator
                Icon(
                  isConnected ? Icons.check_circle : Icons.sync_problem,
                  color: isConnected ? Colors.green : Colors.orange,
                  size: 80,
                ),
                Text(
                  isConnected ? "Connected!" : "Attempting Connection...",
                  style: const TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
                ),

                const SizedBox(height: 40),

                // 2. RESTORED: Sensor Value Display
                const Text("Sensor Reading:", style: TextStyle(color: Colors.grey)),
                StreamBuilder<String>(
                  stream: bleService.dataStream,
                  initialData: "---",
                  builder: (context, dataSnapshot) {
                    return Container(
                      margin: const EdgeInsets.symmetric(vertical: 20),
                      padding: const EdgeInsets.all(30),
                      decoration: BoxDecoration(
                        color: isConnected ? Colors.blue.withValues(alpha: 0.1) : Colors.grey.withValues(alpha: 0.1),
                        borderRadius: BorderRadius.circular(20),
                      ),
                      child: Text(
                        isConnected ? (dataSnapshot.data ?? "0") : "---",
                        style: const TextStyle(fontSize: 48, fontWeight: FontWeight.bold, color: Colors.blue),
                      ),
                    );
                  },
                ),

                if (!isConnected) const SizedBox(height: 40),

                // 3. Reactive Troubleshooting Button
                StreamBuilder<bool>(
                  stream: bleService.scanningStream,
                  initialData: true, // Start as true to hide button on launch
                  builder: (context, scanSnapshot) {
                    final bool isBusy = scanSnapshot.data ?? false;

                    // If connected, show nothing
                    if (isConnected) return const SizedBox.shrink();

                    // If busy (scanning or in 10s cooldown), show a subtle loader or nothing
                    if (isBusy) {
                      return const Column(
                        children: [
                          CircularProgressIndicator(strokeWidth: 2),
                          SizedBox(height: 10),
                          Text("Searching for device...", style: TextStyle(fontSize: 12, color: Colors.grey)),
                        ],
                      );
                    }

                    // Only show the button if disconnected AND not busy
                    return ElevatedButton.icon(
                      onPressed: () => bleService.performManualDiagnostic(),
                      icon: const Icon(Icons.refresh),
                      label: const Text("Manual Re-Connect"),
                    );
                  },
                ),
              ],
            ),
          );
        },
      ),
    );
  }
}
