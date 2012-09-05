"""
TIGO <tuntar.py>,This is an utility function used
to untar tigo's production  files, after all files were
retreived  from the mail box with pop3/imap4 according
to which is available to remote server


Thank to ` Jimmy Retzlaff, 2008 Konstantin Yegupov`
for his exellent UnRAR2 Module to parse  TAR Files
"""

__Author__ = "Alioune Dia"
__Date__   = "2012-09-05 20:10"


from datetime import datetime
import UnRAR2, os , time, thread
_rar_path   = '.'
_rar_files  =  []
def test():
    for  file in os.listdir(_rar_path):
        if '.RAR' in  file:
            path =  '%s'%str(file)
            _rar_files.append(
                path
               )
            

    # basic function to extart tar file, using
    # UnRAR2 module   
    def test2(file):
        rarc = UnRAR2.RarFile(
           file)
        rarc.infolist()
        rarc.extract()
        
    # Extract The list of given tar_files , so
    # Our load_db module can process
    
    for  file in  _rar_files:
        try:
            test2(file)
        except Exception  as e:
            print 'PerHaps email_getter.py has locked  this file\
            No, Problem I will try later '
            print e
            time.sleep(2)


if __name__ =='__main__':
   while True:
        try:
            print 'Starting new thread'
            thread.start_new_thread(
                test,
                ())
        except KeyboardInterrupt, e :
            print 'Stopping the email_getter.py'
            break
        except Exception  as e :
            print e
            print  'Error when starting new thread ,\
            Wating 2 second before restart'
        time.sleep(60*2)
