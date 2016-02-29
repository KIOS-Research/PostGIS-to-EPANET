import os, re, sys, yaml
import psycopg2, numpy
import datetime, json

f = open("wrInpFile.yaml", "r")
cfg = yaml.load(f)

arg=[]
for section in cfg:
    arg.append(cfg[section])

username= "%s" % arg[3]
password= "%s" % arg[4]
DMA= "%s" % arg[0]
host= "%s" % arg[2]
port= "%s" % arg[5]
schema="%s" % arg[6]
dma="%s" % arg[1]

NN="dbname=" + DMA + " host=" + host + " user=" + username + " password=" + password + " port=" + port
# create connection to ices database
conn = psycopg2.connect(NN)

# create a cursor object called cur
cur = conn.cursor()
now = datetime.datetime.now()
pp=str(now)
NETWORK=schema+'dma'+str(dma)+'_'+pp[0:10]+'_'+'AtTime_'+pp[11:13]+'%'+pp[14:16]+'%'+pp[17:19]+'.inp'

# initialize
tanksCh=1
reservoirsCh=1
pumpsCh=1
valvesCh=1
junctionsCh=1

# WGS84 coordinates
# GET ALL LAYERS TABLE FROM DATABASE
# JUNCTIONS
cur.execute("select exists(select * from information_schema.tables where table_schema=%s and table_name=%s)", (schema,'junctions',))
a1=cur.fetchone()[0]

if a1==True:
    cur.execute('''SELECT
      junctions.dc_id,
      junctions.demand,
      junctions.pattern,
      junctions.elevation
    FROM ''' + schema + '.junctions')

    JUNCTIONS1 = cur.fetchall()
else:
    junctionsCh=0

cur.execute("select exists(select * from information_schema.tables where table_schema=%s and table_name=%s)", (schema,'reservoirs',))
a1=cur.fetchone()[0]
if a1==True:
    # RESERVOIRS
    cur.execute('''SELECT
      reservoirs.dc_id,
      reservoirs.head,
      reservoirs.dma
    FROM ''' + schema + '.reservoirs')

    RESERVOIRS1 = cur.fetchall()
else:
    reservoirsCh=0

cur.execute("select exists(select * from information_schema.tables where table_schema=%s and table_name=%s)", (schema,'tanks',))
a1=cur.fetchone()[0]
if a1==True:
    # TANKS
    cur.execute('''SELECT
      tanks.dc_id,
      tanks.elevation,
      tanks.initiallev,
      tanks.minimumlev,
      tanks.maximumlev,
      tanks.diameter,
      tanks.minimumvol,
      tanks.volumecurv,
      tanks.dma
      FROM ''' + schema + '.tanks')

    TANKS1 = cur.fetchall()
else:
    tanksCh=0

# PIPES
cur.execute('''SELECT
  pipes.length,
  pipes.diameter,
  pipes.roughness,
  pipes.minorloss,
  pipes.status,
  pipes.dma,
  pipes.dc_id
  FROM ''' + schema + '.pipes')

PIPES1 = cur.fetchall()

cur.execute("select exists(select * from information_schema.tables where table_schema=%s and table_name=%s)", (schema,'pumps',))
a1=cur.fetchone()[0]
if a1==True:
    # PUMPS
    cur.execute('''SELECT
      pumps.dc_id,
      pumps.pipeTo,
      pumps.From,
      pumps.head,
      pumps.flow,
      pumps.curve,
      pumps.dma
      FROM ''' + schema + '.pumps')

    PUMPS1 = cur.fetchall()
else:
    pumpsCh=0

cur.execute("select exists(select * from information_schema.tables where table_schema=%s and table_name=%s)", (schema,'valves',))
a1=cur.fetchone()[0]
if a1==True:
    # VALVES
    cur.execute('''SELECT
      valves.dc_id,
      valves.pipeTo,
      valves.pipeFrom,
      valves.diameter,
      valves.type,
      valves.setting,
      valves.minorloss,
      valves.dma
      FROM ''' + schema + '.valves')

    VALVES1 = cur.fetchall()
else:
    valvesCh=0

# Vertices
cur.execute('''SELECT ST_AsGeoJSON(geom) FROM ''' + schema + '''.pipes''')

vertices = cur.fetchall()
dd=numpy.zeros(len(vertices))
for i in range(0, len(vertices)):
    if dma in re.sub(r'\s', '', str(PIPES1[i][5])).split(',') or dma=="None":
       a="""%s""" % (vertices[i])#PIPES1[i][6]
       mq=json.loads(a)
       mm=mq['coordinates'][0]
       dd[i]=round(len(mm))

lenVert=int(round(sum(dd)))

lonPipe=numpy.zeros(lenVert)
latPipe=numpy.zeros(lenVert)
Nodes=[]
NodeIndexID=[]#numpy.zeros(lenVert)
lonlatPipeStartEndPoint=[]#numpy.zeros((lenVert,2))
lonlatPipePoints=[]#numpy.zeros((lenVert,2))
PointsALLX=[]
PointsALLY=[]
allPipesID=[]
k=0;PipeFrom=[];PipeTo=[]
#import proj4
for i in range(0, len(vertices)):
    if dma in re.sub(r'\s', '', str(PIPES1[i][5])).split(',') or dma=="None":
       a="%s" % (vertices[i])
       mq=json.loads(a)
       mm=mq['coordinates'][0]
       #for r in range(0, int(dd[i])):
       end=int(dd[i])-1
       allPipesID.append(PIPES1[i][6])
       allPipesID.append(0)
       lonlatPipeStartEndPoint.append([[[mm[0][0], mm[0][1]]],[[mm[end][0], mm[end][1]]]])
       lonlatPipePoints.append(str(["{0:.8f}".format((mm[0][0])), "{0:.8f}".format((mm[0][1]))]))
       PointsALLX.append(mm[0][0])
       PointsALLY.append(mm[0][1])
       NodeIndexID.append(i)
       lonlatPipePoints.append(str(["{0:.8f}".format((mm[end][0])), "{0:.8f}".format((mm[end][1]))]))
       PointsALLX.append(mm[end][0])
       PointsALLY.append(mm[end][1])
       NodeIndexID.append(i+1)
       for r in range(0, int(dd[i])):
           lonPipe[k]=mm[r][0]
           latPipe[k]=mm[r][1]
           k=k+1
    else:
        end=int(dd[i])-1
        allPipesID.append(PIPES1[i][6])
        allPipesID.append(0)
        lonlatPipePoints.append(str(["{0:.8f}".format((mm[0][0])), "{0:.8f}".format((mm[0][1]))]))
        lonlatPipePoints.append(str(["{0:.8f}".format((mm[end][0])), "{0:.8f}".format((mm[end][1]))]))

        NodeIndexID.append(i)
        NodeIndexID.append(i+1)

i=0;b=[] # to b contain coordinates start and point of pipes. start,end,start,end,st...
while i<len(lonlatPipePoints):
    b.append((lonlatPipePoints[i]))
    i=i+1
pointCoords=[];index=[]
pointCoords, index = numpy.unique(b, return_index=True)

# Find all nodes with the same coordinates
allIndices = []
NodeNewIndices=[]
for u in range(0,len(index)):
    allIndices.append([i for i, ltr in enumerate(b) if ltr == b[index[u]]])
    for k, kInd in enumerate(allIndices[u]):
        NodeIndexID[kInd] = str(u+1)
        NodeNewIndices.append(kInd)

# Check Pumps
idPipes=[]
indexPipesTo=[]
indexPipesFrom=[]
for i in range(0,len(PIPES1)):
    idPipes.append(str(i+1))

for u in range(0,len(PUMPS1)):
    i=0
    for s in idPipes:
        if s==PUMPS1[u][1]:
            indexPipesTo.append(i)
        if s==PUMPS1[u][2]:
            indexPipesFrom.append(i)
        i+=1

indexPipe = len(idPipes) + 1
checkPumps=0
if len(indexPipesFrom)<len(PUMPS1):
    checkPumps=len(indexPipesFrom)
    for i in range(0,len(RESERVOIRS1)):
        indexPipesFrom.append(indexPipe+i)
        NodeIndexID.append(str(RESERVOIRS1[i][0]))
        NodeNewIndices.append(indexPipe+i)

if junctionsCh==1 and dma=="None":
    coordinates = cur.fetchall()
    lon=numpy.zeros(len(pointCoords))
    lat=numpy.zeros(len(pointCoords))
    for i in range(0, len(pointCoords)):
        a="""%s""" % (coordinates[i])
        mq=json.loads(a)
        mm=mq['coordinates'][0]
        lat[i]=pointCoords[i][0]
        lon[i]=pointCoords[i][1]

if reservoirsCh==1:
    cur.execute('''SELECT ST_AsGeoJSON(geom) FROM ''' + schema + '''.reservoirs''')
    coordinatesReservoirs = cur.fetchall()
    lonReservoir=numpy.zeros(len(coordinatesReservoirs))
    latReservoir=numpy.zeros(len(coordinatesReservoirs))
    for i in range(0, len(coordinatesReservoirs)):
        if dma in re.sub(r'\s', '', str(RESERVOIRS1[i][2])).split(',') or dma=="None":
            a="""%s""" % (coordinatesReservoirs[i])
            mq=json.loads(a)
            mm=mq['coordinates'][0]
            lonReservoir[i]=mm[0]
            latReservoir[i]=mm[1]

if tanksCh==1:
    lonlatTanks=[]; allTankIndices=[]; tanksID=[]; indexTanksNodes=[]; cc=[]; q=0
    cur.execute('''SELECT ST_AsGeoJSON(geom) FROM ''' + schema + '''.tanks''')
    coordinatesTanks = cur.fetchall()
    lonTank=numpy.zeros(len(coordinatesTanks))
    latTank=numpy.zeros(len(coordinatesTanks))
    for i in range(0, len(coordinatesTanks)):
        if dma in re.sub(r'\s', '', str(TANKS1[i][8])).split(',') or dma=="None":
           a="""%s""" % (coordinatesTanks[i])
           mq=json.loads(a)
           mm=mq['coordinates'][0]
           lonTank[i]=mm[0]
           latTank[i]=mm[1]
           lonlatTanks.append(str(["{0:.9f}".format((mm[0])), "{0:.8f}".format((mm[1]))]))
           allTankIndices.append([u for u, ltr in enumerate(pointCoords) if ltr == str(["{0:.8f}".format((mm[0])), "{0:.8f}".format((mm[1]))])])
           tanksID.append(TANKS1[i][0])
           #if not allTankIndices[q]:
           cc.append(NodeIndexID[index[allTankIndices[q]]]); q+=1

# Write EPANET INP file
TITLE = 'Save EPANET INP file'

# clear all
os.system('cls')

file = open(NETWORK, "w")

file.write("[TITLE]\n")
file.write("Export input file via python\n\n")

file.write("[JUNCTIONS]\n")
file.write(";ID		Elev		Demand		Pattern\n")

if junctionsCh==0:
    for i in range(0,len(index)):
        if str(i+1) not in cc:
            line1= "%s       %s             %s             %s\n" % (str(i+1), 0, 0, '') #ELEVATION
            file.write(line1)
else:
    for i in range(0,len(JUNCTIONS1)):
        junctionsInfo=JUNCTIONS1[i]
        id1=junctionsInfo[0]
        demand1=junctionsInfo[1]
        if isinstance(demand1, float)==False:
           if demand1!=None and demand1!=0:
               #print demand1
               mm=re.findall(r'\b\d+\b', demand1)
               if len(mm)>1:
                  demand1=mm[0]+'.'+mm[1]
           if demand1==None:
              demand1=0
        pattern1=junctionsInfo[2]
        if pattern1==None:
           pattern1=""
        elev1=junctionsInfo[3]
        if elev1==None:
           elev1=0

        line1= "%s       %s             %s             %s\n" % (id1, elev1, demand1, pattern1)
        file.write(line1)

file.write("\n[RESERVOIRS]\n")
file.write(";ID		Head		Pattern\n")
if reservoirsCh==1:
    for i in range(0,len(RESERVOIRS1)):
        if dma in re.sub(r'\s', '', str(RESERVOIRS1[i][2])).split(',') or dma=="None":
            reservoirsInfo=RESERVOIRS1[i]
            id1=reservoirsInfo[0]
            head=reservoirsInfo[1]
            if head==None:
                head=0
            line1= "%s       %s\n" % (id1, head)
            file.write(line1)

file.write("\n[TANKS]\n")
file.write(";ID		Elevation		InitLevel		MinLevel		MaxLevel		Diameter		MinVol		VolCurve\n")

if tanksCh==1:
    for i in range(0,len(TANKS1)):
        if dma in re.sub(r'\s', '', str(TANKS1[i][8])).split(',') or dma=="None":
            tanksInfo=TANKS1[i]
            id1=tanksInfo[0]
            elev1=tanksInfo[1]
            if elev1==None:
               elev1=0
            initlvl1=tanksInfo[2]
            minlvl1=tanksInfo[3]
            maxlvl1=tanksInfo[4]
            diameterlvl1=tanksInfo[5]
            if diameterlvl1==None:
                diameterlvl1=0
            minvol=tanksInfo[6]
            if minvol==None:
               minvol=""
            volcur=tanksInfo[7]
            if volcur==None:
               volcur=""

            line1= "%s       %s             %s             %s             %s             %s              %s            %s \n" % (id1, elev1, initlvl1, minlvl1, maxlvl1, diameterlvl1, minvol, volcur)
            file.write(line1)

file.write("\n[PIPES]\n")
file.write(";ID		Node1		Node2		Length		Diameter		Roughness		MinorLoss		Status\n")

for i in range(0,len(PIPES1)):
    if dma in re.sub(r'\s', '', str(PIPES1[i][5])).split(',') or dma=="None":
       pipesInfo=PIPES1[i]
       id1=str(i+1)
       node1=NodeIndexID[i*2]
       node2=NodeIndexID[i*2+1]

       if node1 in cc:
           node1=tanksID[cc.index(node1)]
       if node2 in cc:
           node2=tanksID[cc.index(node2)]

       length=pipesInfo[0]
       diameter=pipesInfo[1]
       roughness=pipesInfo[2]
       minorloss=pipesInfo[3]
       status=pipesInfo[4]
       line1= "%s       %s             %s             %s             %s             %s             %s             %s \n" % (id1, node1, node2, length, diameter, roughness, minorloss, status)
       file.write(line1)

file.write("\n[PUMPS]\n")
file.write(";ID		Node1		Node2		Parameters\n")

if pumpsCh==1:
    for i in range(0,len(PUMPS1)):
        if dma in re.sub(r'\s', '', str(PUMPS1[i][6])).split(',') or dma=="None":
            pumpsInfo=PUMPS1[i]
            id1=pumpsInfo[0]
            parameters='HEAD'
            curvID =  pumpsInfo[5]
            try:
                n1=allPipesID.index(pumpsInfo[1])
                node1pump=NodeIndexID[n1]
            except:
                node1pump=pumpsInfo[1]
            try:
                n2=allPipesID.index(pumpsInfo[2])
                node2pump=NodeIndexID[n2+1]
            except:
                node2pump=pumpsInfo[2]
            line1= "%s       %s             %s             %s %s\n" % (id1, node2pump, node1pump, parameters,curvID)
            file.write(line1)

file.write("\n[VALVES]\n")
file.write(";ID		Node1		Node2		Diameter		Type	Setting		MinorLoss\n")

if valvesCh==1:
    for i in range(0,len(VALVES1)):
        if dma in re.sub(r'\s', '', str(VALVES1[i][7])).split(',') or dma=="None":
           valvesInfo=VALVES1[i]
           id1=valvesInfo[0]
           n1=allPipesID.index(valvesInfo[1])
           n2=allPipesID.index(valvesInfo[2])
           node1valve=NodeIndexID[n1] # start node
           node2valve=NodeIndexID[n2+1] # end node
           diameterValve=valvesInfo[3]
           typeValve=valvesInfo[4]
           settingValve=valvesInfo[5]
           minorlossValve=valvesInfo[6]
           line1= "%s       %s             %s             %s             %s             %s             %s\n" % (id1, node1valve, node2valve, diameterValve, typeValve, settingValve, minorlossValve)
           file.write(line1)

file.write('''\n[TAGS]

[DEMANDS]
;Junction		Demand		Pattern		Category

[STATUS]
;ID		Status/Setting	''')

file.write('''\n[PATTERNS]
;ID		Multipliers	''')

file.write('''\n[CURVES]

;ID		X-Value		Y-Value\n''')
if pumpsCh==1:
    for i in range(0,len(PUMPS1)):
        curvesInfo = PUMPS1[i]
        curvID=curvesInfo[5]
        curvX=curvesInfo[3]
        curvY=curvesInfo[4]
        if curvX!=None:
            #curvX.split()
            mmX=re.findall(r'\d+..\d+', curvX)
            mmY=re.findall(r'\d+..\d+', curvY)
            bb=len(mmX)
            for u in range(0,len(mmX)):
                line1= "%s       %s             %s\n" % (curvID, mmY[u], mmX[u])
                file.write(line1)

file.write('''\n[CONTROLS]
[RULES]

[ENERGY]
Global Efficiency  	75
Global Price       	0
Demand Charge      	0

[EMITTERS]
;Junction        	Coefficient

[QUALITY]
;Node            	InitQual

[SOURCES]
;Node            	Type        	Quality     	Pattern

[REACTIONS]
;Type     	Pipe/Tank       	Coefficient


[REACTIONS]
Order Bulk            	1.000000
Order Tank            	1.000000
Order Wall            	1
Global Bulk           	0
Global Wall           	0
Limiting Potential    	0
Roughness Correlation 	0

[MIXING]
;Tank            	Model

[TIMES]
Duration           	48:00
Hydraulic Timestep 	0:01
Quality Timestep   	0:01
Pattern Timestep   	0:30
Pattern Start      	0:00
Report Timestep    	0:30
Report Start       	0:00
Start ClockTime    	12:0 AM
Statistic          	None

[REPORT]
Status             	Full
Summary            	No
Page               	0

[OPTIONS]
Units              	CMD
Headloss           	D-W
Specific Gravity   	1
Viscosity          	1
Trials             	50
Accuracy           	0.01
CHECKFREQ          	2
MAXCHECK           	10
DAMPLIMIT          	0
Unbalanced         	Continue 10
Pattern            	1
Demand Multiplier  	1
Emitter Exponent   	0.5
Quality            	AGE
Diffusivity        	1
Tolerance          	9.99999977648258E-02''')

################
file.write("\n[COORDINATES]\n")
file.write(";Node		X-Coord		Y-Coord\n")
if junctionsCh==0:
    x=[];y=[];pp=[]
    for i in range(0,len(index)): #index
        id1=str(i+1)
        if id1 not in cc:
            pp = re.findall(r'\d+..\d+', b[index[i]])
            x = pp[0]
            y = pp[1]
            line1= "%s       %s             %s\n" % (id1, x, y)
            file.write(line1)
else:
    for i in range(0,len(JUNCTIONS1)):
        coordinatesInfo=JUNCTIONS1[i]
        id1=coordinatesInfo[0]
        x=lon[i]
        y=lat[i]
        line1= "%s       %s             %s\n" % (id1, x, y)
        file.write(line1)

if reservoirsCh==1:
    for i in range(0,len(RESERVOIRS1)):
       if dma in re.sub(r'\s', '', str(RESERVOIRS1[i][2])).split(',') or dma=="None":
           reservoirsInfo=RESERVOIRS1[i]
           id1=reservoirsInfo[0]
           x=lonReservoir[i]
           y=latReservoir[i]
           line1= "%s       %s             %s\n" % (id1, x, y)
           file.write(line1)

if tanksCh==1:
    for i in range(0,len(TANKS1)):
        if dma in re.sub(r'\s', '', str(TANKS1[i][8])).split(',') or dma=="None":
           coordinatesInfo=TANKS1[i]
           id1=coordinatesInfo[0]
           x=lonTank[i]
           y=latTank[i]
           line1= "%s       %s             %s\n" % (id1, x, y)
           file.write(line1)
file.write("\n[VERTICES]\n")
file.write(";Link		X-Coord		Y-Coord\n")

ii=0
for i in range(0,len(vertices)):
    id=str(i+1)
    for r in range(0, int(dd[i])):
        x=lonPipe[ii]
        y=latPipe[ii]
        ii+=1
        line1= "%s       %s             %s\n" % (id, x, y)
        file.write(line1)
file.write("\n[END]\n")
file.close()
os.startfile(NETWORK)