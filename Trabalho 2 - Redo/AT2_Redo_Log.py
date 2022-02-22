import re;
import sys;
import json;
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

#Variaveis p/ identificar se existe no .txt
checkvalue = re.compile(r'T[0-9]*,', re.IGNORECASE) #re.IGNORECASE -> ignorar se maiuscula ou minuscula
commit = re.compile(r'commit', re.IGNORECASE)
startckpt = re.compile(r'start ckpt', re.IGNORECASE) 
endckpt = re.compile(r'end ckpt', re.IGNORECASE)
ckpt = re.compile(r'ckpt', re.IGNORECASE)
idfind = re.compile(r'=', re.IGNORECASE) 
extracT = re.compile(r'(?!commit\b)(?!CKPT\b)(?!Start\b)\b\w+', re.IGNORECASE) #Ignora as palavras descritas e coloca as demais em uma lista com .findall
words = re.compile(r'\w+', re.IGNORECASE)   #Utilizado p/ pegar o valor das variaveis

# Criação da tabela e inserts iniciais ------------------------------------------------------------------------
#Conta quantos ids vão ter no início
countID = 0
maiorId = 0
variaveisCreate = [] #Array das varíaveis que devem ser iniciadas
for item in arquivolist:
    if(idfind.search(item)):
        countID+=1
        print(item[0])
        variaveisCreate.append(item[0]) if item[0] not in variaveisCreate else variaveisCreate ## APPEND SO SE NAO TEM NO ARRAY
        if int(item[2]) > int(maiorId):  #Busca pelo maior Id do arquivo
            maiorId = item[2]

inserts = [] #Array de insert into de cada id

k = 0 #linha atual do arquivo
i = 0 #utilizado para comparar se já chegou no ID mais alto do arquivo
while i != maiorId:
    valores = words.findall(arquivolist[k])
    idAtual = valores[1]
    stringVariaveis = "INSERT INTO redo (id,"
    stringValores = " VALUES ("+idAtual+", "
    for j in range(0,countID,1):
        linhaAtual = words.findall(arquivolist[j])
        if idAtual == linhaAtual[1]:
            stringVariaveis += " "+linhaAtual[0]+","
            stringValores += " "+linhaAtual[2]+","

    stringVariaveis = stringVariaveis[:-1]+")"
    stringValores = stringValores[:-1]+")"
    inserts.append(stringVariaveis+stringValores)
    
    i = idAtual
    k += 1

stringCreateTable = "CREATE TABLE IF NOT EXISTS redo (id INT,"

#cria a string para rodar o CreateTable
for i in range(0,len(variaveisCreate),1):
    if i==len(variaveisCreate)-1: stringCreateTable += " "+variaveisCreate[i]+" INT)"
    else: stringCreateTable += " "+variaveisCreate[i]+" INT,"

print(stringCreateTable)
cur.execute("DROP TABLE IF EXISTS redo")
cur.execute(stringCreateTable)

#roda os insert into
for stringInsert in inserts:
    cur.execute(stringInsert)
    
conn.commit()
