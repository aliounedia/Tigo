Tigo
====

Tigo


Author 
Alioune Dia
Date 
2012-09-04 :23:00


*

Ce module est utilise pour traiter tous les fichiers de TiGO 
pour l'emission des appels. Il utilise  le module UNRAR2 
de l'exellent programmeur  ` Jimmy Retzlaff, 2008 
Konstantin Yegupov`

Je lui remercie pour le travail qu'il a effectue. Ce module
comprend donc trois scripts

* send.py
	
Est utlise pour enyoyer des emails une fois que tous les 
fichiers sont `Unrares` et que le chargement en base s'est
bien deoulé

* Load.py

Module pour charger les fichiers dans la base et leur renommage
,ce module est passif, il tourne en demon et check les fichiers
a charger, puis procede aux chargement. Il peut arriver que 
le fichier soit utilise par un autre process dans ce
cas `Load.py` ne fait que passer et revient plus tard.

* unrar.py

Utilise l'exellent  UNRAR2  de `Jimmy Retzlaff`


* Run
	* Python load.py
	* python unrar.py


Enjoy
Ad
