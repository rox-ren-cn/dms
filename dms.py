import os, glob
import tarfile
import zipfile
#import rarfile
import patoolib
import gzip
import md5
import hashlib
import shutil
import sqlite3

rootdir = 'D:/SVN/BOA_V2/delivery'
tmpdir = 'D:/SVN/BOA_V2/DMS/tmp'

def unpack(path):
    for subdir, dirs, files in os.walk(path):
        print 'subdir[' + subdir + ']'
        print 'dirs[' + ",".join(dirs) + ']'
        print 'files[' + ",".join(files) + ']'
        for file in files:
            f = os.path.join(subdir, file)
            print 'file[' + f + ']'
            if os.path.isfile(f):
                print 'file[' + f + ']'
                if file.endswith('gz'):
                    fo = f[:-3]
                    with gzip.open(f, 'rb') as f_in, open(fo, 'wb') as f_out :
                        shutil.copyfileobj(f_in, f_out)
                    f_in.close();
                    f_out.close();
                    os.remove(f)
                    print 'gunzip ' + fo
                    return False
                elif file.endswith('tar'):
                    tfile = tarfile.open(f, 'r')
                    tfile.extractall(subdir)
                    tfile.close()
                    os.remove(f)
                    print 'untar ' + f
                    return False
                elif file.endswith('zip'):
                    zip_ref = zipfile.ZipFile(f) # create zipfile object
                    zip_ref.extractall(subdir) # extract file to dir
                    zip_ref.close() # close file
                    os.remove(f) # delete zipped file     
                    print 'unzip ' + f
                    return False
                elif file.endswith('rar'):
                    #rar_ref = rarfile.RarFile(f)
                    #rar_ref.extractall(subdir)
                    #rar_ref.close()
                    patoolib.extract_archive(f, outdir=subdir)
                    os.remove(f)
                    print 'unrar ' + f
                    return False
            else:
                unpack(file)
                    
    print 'unpack done!!'
    return True

def calcMd5(path, patchno):
    data = []
    di = dict()
    for subdir, dirs, files in os.walk(path):
#        print 'subdir[' + subdir + ']'
#        print 'dirs[' + ",".join(dirs) + ']'
#        print 'files[' + ",".join(files) + ']'
#        filess = filter(lambda f: not f.endswith('.doc') , patchFilter)
        for file in files:
            f = os.path.join(subdir, file)
            print '\tfile : ' + f
            hash = hashlib.md5()
            with open(f, "rb") as ff:
                for chunk in iter(lambda: ff.read(4096), b""):
                    hash.update(chunk)

            if not di.has_key(hash.hexdigest()):
                data.append( (hash.hexdigest(), file, 'arch', patchno, f) )
                di[hash.hexdigest()] = (hash.hexdigest(), file, 'arch', patchno, f)

    print 'calc MD5 done!!'

    conn = sqlite3.connect('dms.db')
    c = conn.cursor()

    # Larger example that inserts many records at a time
    c.executemany('INSERT INTO MD5 VALUES (?,?,?,?,?)', data)
    conn.commit()
    conn.close()

def processOnePatch(path, flag = True):
    patchno = os.path.basename(path)
    print 'Processing patchc : ' + patchno
    if flag:
        shutil.rmtree(tmpdir, True)
        if os.path.isdir(path):
            shutil.copytree(path, tmpdir)
        else:
            os.makedirs(tmpdir)
            shutil.copy(path, tmpdir)

        while not unpack(tmpdir):
            x=1

    calcMd5(tmpdir, patchno)

patchFilter = glob.glob('D:/SVN/BOA_V2/delivery/DN[456]*')
patches = filter(lambda f: os.path.isdir(f) or os.path.isfile(f), patchFilter)

for file in patches :
    processOnePatch(file)

#time.sleep(5.5)    # pause 5.5 seconds

