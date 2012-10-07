"""
TIGO <load.py>,This is an utility function used
to load contents files into the database after
files  were retreived  from  mail box and untared
using <untar.py> 

Thank to ` Jimmy Retzlaff, 2008 Konstantin Yegupov`
for the exellent UnRAR2 Module to parse  TAR Files

"""

__Author__ = "Alioune Dia"
__Date__   = "2012-09-05 20:10"

from   datetime import date , datetime
import pyodbc , time , thread , os
import send 

# Point To The current directory
WORKDIR ='.'

# One segment peer Month For Tigo
_dayMonth                =  date.today().strftime('%Y%m01')
_SEGMENT_NAME_AGENCES    = "TIGO_CUSTOMER_PRICE_CALL_%s_AGENCES_"%(
                         _dayMonth)

_SEGMENT_NAME_POS        = "TIGO_CUSTOMER_PRICE_CALL_%s_POS_"%(
                         _dayMonth)
_SEGMENT_NAME_UNKNOWN    = "TIGO_CUSTOMER_PRICE_CALL_%s_UNKNOWN_"%(
                         _dayMonth)



# ['768495662', 'AGENCE Louga', 'VENTES', '2012-09-04 00:00:00.000',
# 'BAYE D IAMIL', '', 'NEW',
# 'TIGO_CUSTOMER_PRICE_CALL_20120901_AGENCES__NEW' ,'2012-09-05']


# Main    =====             list 
# Nom                       =>4
# Prenom                    => ''
# tel                       =>0
# fichier                   =>7
# Distributeur              =>1
# Adr1                      =>2
# NouvAdr2                  =>3
# NouvAdr3                  =>6
# Main_DateChargment        =>8
# main_called               =>9
# main_locked               =>10
# main_codefinappel         =>11
# main_coderefus            =>12


_QUERY             ="""
insert  into main(
    Main_Nom,
    Main_Prenom, 
    main_tel, 
    main_fichier,
    Main_Distributeur , 
    Main_Adr1,
    Main_NouvAdr2,
    Main_NouvAdr3,
    Main_DateChargment,
    main_called, 
    main_locked , 
    main_codefinappel, 
    main_coderefus)
    values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


STRING_CONNECTION  = "DRIVER={SQL Server};\
SERVER=X;DATABASE=X;UID=X;PWD=X"

# Initialize a connection  from  database using
# STRING_CONNECTION

def get_connection():
    try:
        return pyodbc.connect(
                   STRING_CONNECTION,
                   autocommit =True)
    except:
        raise  Exception(
    'Cannot connect to db')

# Get Cursor
def cur():
    return  get_connection().cursor()


# Soring data from  db
def storeDb(data):
   c  =cur()
   nice_lines =[]
   for line in data:
       nice_lines.append(
           [
            line[4],
            '',
            line[0],
            line[7],
            line[1],
            line[2],
            line[3],
            line[6],
            line[8],
            line[9],
            line[10],
            line[11],
            line[12]
            ]
           )
   print nice_lines
   time.sleep(10)
   
   c.executemany(
        _QUERY,
        nice_lines)

# Clean The database , nothing for this moment
def cleanDb():
   pass


#Rename file to avoid files to be rehandled again
def renamefile(old_file):
    try:
        file, ext = os.path.splitext(old_file)
        new_file = '%s_Traite_%s' %(file , ext)
        os.rename(old_file ,new_file )
    except OSError, e:
        print 'Cannot rename file'
        pass

    
# Send email 
def sendMail(data):
    map = {}
    for line in data:
        if not line[7] in map:
            map[line[7]] = 1
        else:
            map[line[7]] +=1


    flat     = '<br/>'.join( ['%s ** %s' %(one, two)
                          for (one, two) in map.items() ])

    
    
    to       = ['adia@pcci.sn',
                'dia.aliounes@gmail.com']
           
    subject   ="[Tigo] Chargement de fichiers %s """%\
                 date.today().strftime('%Y%m%d')
    
   
                    
    Mail ="""
Bonjour,<br/>
les chargements sur le l' emission des appels de Tigo<br/>.
Pour les segments suivants :<br/>
%s
<br/>
<br/>
<br/>
Cordialement<br/>
Service Informatique.<br/>
    """%(flat)

    send.send(att_path = None , body =Mail,
                    to = to , subject =subject)
     

    
# Bulk
def test():
    for file in os.listdir(WORKDIR):
        print file
        base, ext    = os.path.splitext(file)
        print
        base_traite  = '%s_Traite_%s' % (base, ext)
        if os.path.exists(base_traite):
            continue
        
        # This is a RAR files do not handle
        if  '.RAR' in file or  'load.py' in file \
           or  'unrar.py' in file  or  'send.py' in file\
               or '_Traite_' in file:
                 continue
        print  'File ' , file 
        data =prepare_data(file)
        if not len(data):
            renamefile(file)
            continue


        # Ce ci est un Fix, car il semble que dans l'extraction
        # quotidient que Tigo  envoi, parfois on y trouve un ou
        # deux fihiers mals formes


        # Fix
        #=====

        # Je ne peux pas courir le risque que des fichiers
        # 10 /ne soitent pas charges parceque il en exist
        # 1  mal formes .Le plus sur c est de charger les 9
        # et sauter le 1
        # Comme cela les CCX risquent pas de manquer de fiches
        # le matin :)
        
        # When all data items have'nt the same length, it
        # may raise  un error here so , when these happen
        # this should not block others files to be handled
        try:
            storeDb(data)
            sendMail(data)
            cleanDb()
            renamefile(file)
        except Exception, e:
            # We are sure that the number of file to be handled
            # is 4 , so we will check tomorrow into the renamed
            # files if it  exists some files to handle.
            renamefile(file)
            print e
            


# Prapare to for db
def prepare_data(file , *args , **kwargs):
    db_lines     = []
    segment_name = []


    # OnLoad

    # Main_Called = 'N' ,
    # Main_Locked = 0 ,
    # Main_CodeFinappel ='',
    # Main_CodeRefus =''
    
    # to complete a db_inline
    completed_lines = ['N', '0' ,'', '' ]

    # Handle Tigo file
    with  open(file, 'r')  as tigo_file:
        lines =    tigo_file.readlines()
        for line in lines:
            db_line = []
            if ';' in line:
                db_line.extend([ a.strip('\n')
                                 for  a in
                                 line.split(';')])
                
                if '_POS_' in file:
                    segment_name  = _SEGMENT_NAME_POS
                elif  '_AGENCES_' in file:
                    segment_name  = _SEGMENT_NAME_AGENCES
                else:
                    segment_name  = _SEGMENT_NAME_UNKNOWN

                # The last last line item is concatenated
                # to the filename
                segment_name ='%s_%s'%(segment_name, db_line[-1])
                
                db_line.append(segment_name)

                # Day Stored 
                db_line.append(date.today().strftime('%Y-%m-%d'))
                db_line.extend(completed_lines)
                db_lines.append(db_line)

        return db_lines
                   

if __name__ =='__main__':
   while True:
        try:
            print 'Starting new thread'
            thread.start_new_thread(
                test,
                ())
        except KeyboardInterrupt, e :
            print 'Stopping the email_getter.py'
        except Exception, e:
            print e
            print  'Error when starting new thread ,\
            Wating 10 second before restart'
        time.sleep(60*2)
