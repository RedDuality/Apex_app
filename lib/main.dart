import 'package:apex_app/archive_service.dart';
import 'package:apex_app/connection_service.dart';
import 'package:apex_app/main_page.dart';
import 'package:flutter/material.dart';

void main() {
  final bleService = ConnectionService();
  final archiveService = ArchiveService(bleService.packetStream);

  // This line is critical for the automatic start!
  bleService.start();

  runApp(MaterialApp(
    theme: ThemeData(useMaterial3: true, colorSchemeSeed: Colors.blue),
    home: MainPage(bleService: bleService, archiveService: archiveService),
  ));
}