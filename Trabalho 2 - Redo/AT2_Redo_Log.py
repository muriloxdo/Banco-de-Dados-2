import re;
import sys;
import json;
import mysql.connector;
import psycopg2;

conn = psycopg2.connect(
    host="localhost",
    database="bd2",
    user="postgres",
    password="ola"
)

cur = conn.cursor()

arquivo = open('teste02.txt', 'r')      #cria uma lista com o .txt
arquivolist = list(arquivo)     
REDO = []                       #salva quem vai ser feito REDO         
TRANSACOES = []                 #todas as transações  

