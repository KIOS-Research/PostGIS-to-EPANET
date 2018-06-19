PostGIS to EPANET
==================================

Export to EPANET INP files with python and postgres database.
Run PostGIStoEPANET.exe

wnInpFile.yalm 
-----------------------

junctions_shp - name of junctions shapefile

junctions fields: dc_id, demand, pattern, elevation
#
reservoirs_shp - name of reservoirs shapefile

reservoirs fields: dc_id, head, dma
#
tanks_shp - name of tanks shapefile

tanks fields: dc_id, elevation, initiallev, minimumlev, maximumlev, diameter, minimumvol, volumecurv, dma
#
pipes_shp - name of pipes shapefile

pipes fields: dc_id, diameter, roughness, minorloss, status, dma
#
pumps_shp - name of pumps shapefile

pumps fields: dc_id, From, head, flow, curve, pipeTo, dma
#
valves_shp - name of valves shapefile

valves fields: dc_id, pipeTo, pipeFrom, diameter, type, setting, minorloss, dma
#
