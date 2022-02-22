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
#sys.exit("Sartei")

#--------------------------------------------------------------------------------------------------------------

#começo da lógica de REDO -------------------------------------------------------------------------------------
ultimoStart = None
ultimoEnd = None
flagStart = None

#verifica qual são os últimos start e end checkpoints
for i in range(0,len(arquivolist),1):
    if(startckpt.search(arquivolist[i])): 
        auxStart = i
        flagStart = True            #flag para somente mudar o start se existe um end após
    if(endckpt.search(arquivolist[i])): 
        if(flagStart):
            ultimoEnd = i
            ultimoStart = auxStart
            flagStart = None

    #append nas transações existente
    if(checkvalue.search(arquivolist[i])): TRANSACOES.append(extracT.findall(arquivolist[i])[0]) if extracT.findall(arquivolist[i])[0] not in TRANSACOES else TRANSACOES

#quais transações fazem redo
if ultimoStart!=None and ultimoEnd!=None: 
    for i in range(ultimoStart, len(arquivolist), 1): #do fim do arquivo até o primeiro Start CKPT
        if commit.search(arquivolist[i]):  #Procura commit
            REDO.append(extracT.findall(arquivolist[i])[0])
else:
    for linha in arquivolist: #Verificar os casos e criar as listas de REDO
        if commit.search(linha):  #Procura commit
            REDO.append(extracT.findall(linha)[0])


#Printando quem aplicou ou não REDO    
for item in TRANSACOES: 
    if(item in REDO): print("Aplicou Redo: "+item)
    else: print("Não aplicou Redo: "+item)


#cria uma lista de atual
listaUpdate = {}
for redo in REDO:
    for j in range(len(arquivolist)-1, -1, -1):
        linha = arquivolist[j]

        if((extracT.findall(linha)[0]==redo) and ckpt.search(linha)==None and checkvalue.search(linha)): #verifica se o T está no vetor REDO, não é checkpoint e se é uma linha de valores <T1,1,A,30>
            try: 
                listaUpdate[extracT.findall(linha)[0]+"-"+extracT.findall(linha)[1]+"-"+extracT.findall(linha)[2]]
            except: 
                listaUpdate[extracT.findall(linha)[0]+"-"+extracT.findall(linha)[1]+"-"+extracT.findall(linha)[2]] = {'id': extracT.findall(linha)[1],'atributo': extracT.findall(linha)[2],'valor': words.findall(linha)[3]}

print(json.dumps(listaUpdate, sort_keys=True, indent=4))

for item in listaUpdate: #Iniciar primeiros valores das variáveis (A B C...)
    objeto = listaUpdate[item]
    select = "SELECT "+objeto['atributo']+" FROM redo WHERE id="+objeto['id']
    cur.execute(select)
    myresult = cur.fetchall()

    for x in myresult:
        if(int(x[0])!=int(objeto['valor'])):
            update = "UPDATE redo SET "+objeto['atributo']+"="+objeto['valor']+" WHERE id="+objeto['id']
            #print(update)
            cur.execute(update)

conn.commit()

arquivo.close()

conn.close()

