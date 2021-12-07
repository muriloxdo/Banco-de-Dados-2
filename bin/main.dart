/**
 * José Timm
 * Murilo Xavier
 * Rodrigo Vogt
 */
import 'dart:io';
import 'dart:convert';

import 'package:postgres/postgres.dart';

void main(List<String> arguments) async {
  final conn = PostgreSQLConnection(
    'localhost',
    5435,
    'job',
    username: 'postgres',
    password: 'ola',
  );
  await conn.open();

  print('Connected to Postgres database...');

  print('Drop Table...');
  await conn.query('''
    DROP TABLE teste;
  ''');  
  
  // NOTE Do this as part of DB setup not in application code!
  // Create data
  print('Create Table...');
  await conn.query('''
   CREATE TABLE teste (
      id varchar(500) PRIMARY KEY NOT NULL,
      title varchar(300) NOT NULL,
      score int NOT NULL,
      url varchar(500) NOT NULL,
      comms_num int NOT NULL,
      created int NOT NULL,
      body text NOT NULL,
      timestamp date NOT NULL
    )
   ''');
 
/*
  Stopwatch stopwatch = new Stopwatch()..start();
  print('INSERINDO EXPLICITA...');
  // Populate customers table
  await conn.transaction((ctx) async {
    final mockStr = await File('../csvjson.json').readAsString();
    final mockData = json.decode(mockStr);
    final mockDataStream = Stream.fromIterable(mockData);

    await for (var row in mockDataStream) {
      await ctx.query('''
        INSERT INTO teste (title, score, id, url, comms_num, created, body, timestamp)
        VALUES (@title, @score, @id, @url, @comms_num, @created, @body, @timestamp)
      ''', substitutionValues: {
        'title': row['title'], 
        'score': row['score'], 
        'id': row['id'], 
        'url': row['url'], 
        'comms_num': row['comms_num'], 
        'created': row['created'], 
        'body': row['body'], 
        'timestamp': row['timestamp'],
      });
    }
  });
  print('EXPLICITA Inserida em: ${stopwatch.elapsed}');
*/

  Stopwatch stopwatch = new Stopwatch()..start();
  print('INSERINDO IMPLICITA...');
  await conn.transaction((ctx) async {
    final mockStr = await File('../csvjson.json').readAsString();
    final mockData = json.decode(mockStr);
    final mockDataStream = Stream.fromIterable(mockData);

    var quero = "INSERT INTO teste (title, score, id, url, comms_num, created, body, timestamp) VALUES ";

    var cond = '';
    await for (var row in mockDataStream) {
        cond += " ('" + row['title'].toString() + "', '" 
        + row['score'].toString()   + "', '" 
        + row['id'].toString()                 + "', '" 
        + row['url'].toString()                + "', '"   
        + row['comms_num'].toString() + "', '" 
        + row['created'].toString()   + "', '" 
        + row['body'].toString()               + "', '" 
        + row['timestamp'].toString()          + "'), ";
    }

    cond = cond.substring(0, cond.length-2);
    quero += cond;
    //print('$quero');
    await ctx.query(quero);
  });
  print('IMPLICITA Inserida em: ${stopwatch.elapsed}');


  await conn.close();
}
