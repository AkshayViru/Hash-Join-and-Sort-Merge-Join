import os
import sys
import time
import heapq

# start_time = time.time()

R_PATH = ""
S_PATH = ""
R_FNAME = ""
S_FNAME = ""
METHOD = "sort"
M = 0
BLOCK_SIZE = 100 #Number of Records per block
B_R = 0 #Number of blocks for R
B_S = 0 #Number of blocks for S
R_ROWCOUNT = 0
S_ROWCOUNT = 0
R_BUF_SZ = [] # Subfile numbers of R
S_BUF_SZ = [] # Subfile numbers of S
heap = []
r_fp = []
s_fp = []
out_filename = ""

class heap_object(object):
    def __init__(self, key, row, filename, isR):
        self.key = key
        self.row = row
        self.filename = filename
        self.isR = isR
    def __lt__(self, other):
        if(self.key < other.key):
            return True
        elif(self.key > other.key):
            return False
        return False

def process_filepath(path):
    if(path[0] == '/'):
        return path
    else:
        if(path[0] == '.'):
            return os.getcwd() + path[1:]
        return os.getcwd() + '/' + path

def get_file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def process_args(argv):
    global R_PATH, S_PATH, M, METHOD, R_BUF_SZ, S_BUF_SZ, R_FNAME, S_FNAME, R_ROWCOUNT, S_ROWCOUNT, B_R, B_S, out_filename

    R_PATH = process_filepath(argv[1])
    S_PATH = process_filepath(argv[2])
    R_FNAME = os.path.basename(R_PATH).split('.')[0]
    S_FNAME = os.path.basename(S_PATH).split('.')[0]
    out_filename =  R_FNAME + '_' + S_FNAME + '_join.txt'

    M = int(argv[4])
    R_BUF_SZ = [0 for _ in range(M)]
    S_BUF_SZ = [0 for _ in range(M)]

    if(os.path.isfile(R_PATH) == False):
        print('[ERROR] File not found: '+R_FNAME)
        sys.exit(1)
    if(os.path.isfile(S_PATH) == False):
        print('[ERROR] File not found: '+S_FNAME)
        sys.exit(1)

    R_ROWCOUNT = get_file_len(R_PATH)
    S_ROWCOUNT = get_file_len(S_PATH)
    B_R = (R_ROWCOUNT + BLOCK_SIZE - 1) // BLOCK_SIZE
    B_S = (S_ROWCOUNT + BLOCK_SIZE - 1) // BLOCK_SIZE

    if(argv[3].lower() == 'hash'):
        METHOD = 'hash'
        
    if(M < 3):
        print('[ERROR] Insufficient Buffers')
        sys.exit(1)
    
    if(METHOD == 'hash' and min(B_R, B_R) > M * M):
        print('[ERROR]: Not enough buffers (min(B(R), B(S)) > M x M)')
        sys.exit(1)
    elif(METHOD == 'sort' and B_R + B_R >= M * M):
        print('[ERROR]: Not enough buffers min(B(R) + B(S) >= M x M)')
        sys.exit(1)

def gethash(filename, line, n):
    line = line.split()
    filename = filename[0]

    if(len(line) != 2):
        print('Invalid format in input file')
        sys.exit(1)

    if(filename == 'R'):
        key = line[1]
    else:
        key = line[0]
    
    key = key.strip().rstrip()

    hash = 0
    for c in str(key):
        hash = 101 * hash  +  ord(c)
        
    return hash%n

def pre_getnext():
    if(METHOD == 'sort'):
        global r_fp, s_fp
        r_filenames = ['R' + str(x)+'sorted.txt' for x in range(R_BUF_SZ)]
        r_fp = {filename: open(filename, 'r+') for filename in r_filenames}

        cnt = 0
        while(cnt < BLOCK_SIZE):
            for i in r_fp:
                try:
                    line = r_fp[i].readline()
                except:
                    pass
                if(len(line) == 0):
                    r_fp[i].close()
                else:
                    key = line.strip().rstrip().split()[1]
                    heapq.heappush(heap, heap_object(key, line, i, True))
                    cnt += 1
        
        s_filenames = ['S' + str(x)+'sorted.txt' for x in range(S_BUF_SZ)]
        s_fp = {filename: open(filename, 'r+') for filename in s_filenames}

        cnt = 0
        while(cnt < BLOCK_SIZE):
            for i in s_fp:
                try:
                    line = s_fp[i].readline()
                except:
                    pass
                if(len(line) == 0):
                    s_fp[i].close()
                else:
                    key = line.strip().rstrip().split()[0]
                    heapq.heappush(heap, heap_object(key, line, i, False))
                    cnt += 1

    out = open(out_filename, 'w')
    out.close()

def openhash(filename):
    if(filename[0].lower() == 'r'):
        inp = open(R_PATH, 'r')
    else:
        inp = open(S_PATH, 'r')

    hashTable = [[] for _ in range(M-1)]

    for line in inp:
        hashval = gethash(filename, line, M-1)
        if(len(hashTable[hashval]) == BLOCK_SIZE):
            f = open(filename + str(hashval) + '.txt', 'a')
            for row in hashTable[hashval]:
                f.write(row)
            hashTable[hashval] = []
            f.close()

        hashTable[hashval].append(line)

        if(filename == 'R'):
            R_BUF_SZ[hashval] += 1
        else:
            S_BUF_SZ[hashval] += 1

    for i in range(len(hashTable)):
        if(len(hashTable[i])):
            f = open(filename + str(i) + '.txt', 'a')
            for row in hashTable[i]:
                f.write(row)
            hashTable[i] = []
            f.close()

def get_next_hash(i):

    r_fname = 'R' + str(i) + '.txt'
    s_fname = 'S' + str(i) + '.txt'
    
    if(R_BUF_SZ[i] == 0 or S_BUF_SZ[i] == 0):
        return
    
    build = r_fname
    probe = s_fname

    if(R_BUF_SZ[i] > S_BUF_SZ[i]):
        build = s_fname
        probe = r_fname

    hashTable = [[] for _ in range(M-2)]

    with open(build, 'r') as f:
        for line in f:
            hashval = gethash(build, line, M-2)
            line = line.rstrip()
            hashTable[hashval].append(line)
        f.close()
    
    outbuf = []

    with open(probe, 'r') as f:
        for line in f:
            hashval = gethash(probe, line, M-2)
            if(len(hashTable[hashval]) == 0):
                continue

            line = line.rstrip()

            for i in hashTable[hashval]:
                if(len(outbuf) == BLOCK_SIZE):
                    out = open(out_filename, 'a')
                    for j in outbuf:
                        out.write(j + '\n')
                    out.close()
                    outbuf = []
                
                if(build[0] == 'R'):
                    R_key = i.split()[1].strip().rstrip()
                    S_key = line.split()[0].strip()
                    if(R_key == S_key):
                        outbuf.append(i + ' ' + line.split()[1])
                else:
                    R_key = line.split()[1].strip()
                    S_key = i.split()[0].strip().rstrip()
                    if(R_key == S_key):
                        outbuf.append(line + ' ' + i.split()[1])
        f.close()
    
    if(len(outbuf)):
        out = open(out_filename, 'a')
        for j in outbuf:
            out.write(j + '\n')
        out.close()

def create_subfiles(filepath, filename):
    count = 0
    filenum = 0
    TUPLES_PER_SUBFILE = BLOCK_SIZE * M
    f = open(filename + str(filenum)+'sorted.txt', 'w')
    f.close()

    with open(filepath) as input:
        for line in input:
            if(count == 0):
                f = open(filename + str(filenum)+'sorted.txt', 'w')
                filenum += 1
            f.write(line)
            count += 1
            if(count == TUPLES_PER_SUBFILE):
                count = 0
                f.close()
                
        if(f.closed == False):
            f.close()

    input.close()

def sort_subfiles(TOTAL_SUBFILES, sort_index, filename):
    for i in range(TOTAL_SUBFILES):
        cur = filename + str(i)+'sorted.txt'
        table= []
        with open(cur, 'r+') as f:
            for line in f:
                row = line.split()
                table.append(row)

            table.sort(key = lambda table: table[sort_index])

            f.truncate(0)
            f.seek(0)
            
            for row in table:
                for cell in range(len(row)):
                    f.write(row[cell])
                    if(cell != len(row)-1):
                        f.write(' ')
                    else:
                        f.write('')
                f.write('\n')
            f.close()

def opensort(filename):
    global R_BUF_SZ, S_BUF_SZ
    if(filename == 'R'):
        create_subfiles(R_PATH, 'R')
        R_BUF_SZ = (((R_ROWCOUNT + BLOCK_SIZE - 1) // BLOCK_SIZE) + M - 1 ) // M
        sort_subfiles(R_BUF_SZ, 1, 'R')
    else:
        create_subfiles(S_PATH, 'S')
        S_BUF_SZ = (((S_ROWCOUNT + BLOCK_SIZE - 1) // BLOCK_SIZE) + M - 1 ) // M
        sort_subfiles(S_BUF_SZ, 0, 'S')

def get_next_sort():
    
    if(len(heap) == 0):
        return False
    
    global r_fp, s_fp
    curkey = heap[0].key
    s_tuples = []
    r_tuples = []

    while(True):
        if(len(heap) == 0 or heap[0].key != curkey):
            break

        r_itr = len(r_tuples)
        s_itr = len(s_tuples)

        while(len(heap) and heap[0].key == curkey):
            obj = heapq.heappop(heap)
            if(obj.isR):
                r_tuples.append(obj)
            else:
                s_tuples.append(obj)

        while(r_itr < len(r_tuples)):
            obj = r_tuples[r_itr]
            try:
                line = r_fp[obj.filename].readline()
            except:
                r_itr += 1
                continue
            if(len(line) == 0):
                r_fp[obj.filename].close()
                del r_fp[obj.filename]
            else:
                key = line.strip().rstrip().split()[1]
                heapq.heappush(heap, heap_object(key, line, obj.filename, True))
            r_itr += 1

        while(s_itr < len(s_tuples)):
            obj = s_tuples[s_itr]
            try:
                line = s_fp[obj.filename].readline()
            except:
                s_itr += 1
                continue
            if(len(line) == 0):
                s_fp[obj.filename].close()
                del s_fp[obj.filename]
            else:
                key = line.strip().rstrip().split()[0]
                heapq.heappush(heap, heap_object(key, line, obj.filename, False))
            s_itr += 1
    
    out = open(out_filename, 'a')

    # print(len(r_tuples), end = ' ')
    # print(len(s_tuples))

    for i in r_tuples:
        for j in s_tuples:
            r_val = i.row.rstrip()
            out.write(r_val + ' ')
            s_val = j.row.split()[1].rstrip()
            out.write(s_val + '\n')
    
    out.close()

    return True
        
def close():
    if(METHOD == 'sort'):
        for i in range(R_BUF_SZ):
            os.remove('R'+str(i)+'sorted.txt')
        for i in range(S_BUF_SZ):
            os.remove('S'+str(i)+'sorted.txt')
    else:
        for i in range(M):
            if(R_BUF_SZ[i]):
                os.remove('R'+str(i)+'.txt')
            if(S_BUF_SZ[i]):
                os.remove('S'+str(i)+'.txt')

if __name__ == "__main__":

    if(len(sys.argv) != 5):
        print('Invalid Argument Count')
        sys.exit(1)
    
    process_args(sys.argv)
    
    if(METHOD == 'hash'):
        openhash('R')
        openhash('S')
        pre_getnext()
        for i in range(M):
            get_next_hash(i)
        close()
    else:
        opensort('R')
        opensort('S')
        pre_getnext()
        while(get_next_sort()):
            pass
        close()

    # print("--- %s seconds ---" % (time.time() - start_time))