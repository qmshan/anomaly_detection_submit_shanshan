from sys import argv
import sys
from enum import Enum
from sets import Set
from Queue import Queue
import time
import heapq
import math

class EV_TYPE(Enum):
    PURCHASE = 1
    FRIEND = 2
    UNFRIEND = 3
    NONE = 4

"""
log class is used to store log info after string parsing
"""
class log_item(object):
    def __init__(self, event, stamp, usr1, usr2, value, line):
        self.event = event
        self.stamp = stamp
        self.usr1 = usr1
        self.usr2 = usr2
        self.value = value
        self.line = line

    #for debug print out
    def print_out(self):
        print '---------New Log---------------'
        print self.line
        print "event_type:", self.event
        print "timestamp:", self.stamp
        print "user #1   :", self.usr1
        print "user #2   :", self.usr2
        print "amount    :", self.value

"""
parser: The service to transform one line of log stream from jason into log
    item
"""
class Parser(object):
    def timeTrans(self,s):
        return int(time.mktime(time.strptime(s, '%Y-%m-%d %H:%M:%S')))
    
    def process(self, line):
        line = line.strip();
        line = line.strip('{}')
        #get log line
        even,stime,str1,str2 = line.split(',')
        #get time stamp
        stime = stime.strip()
        stime = stime.strip("\"timestamp\":")
        stime = stime.strip()
        stime = stime.strip('\"')
        stamp = self.timeTrans(stime)
        #get event type
        s1,type = even.split(':');
        if "\"purchase\"" == type:
            event = EV_TYPE.PURCHASE
        elif "\"befriend\"" == type:
            event = EV_TYPE.FRIEND
        elif "\"unfriend\"" == type:
            event = EV_TYPE.UNFRIEND
        else:
            print 'ERROR: parser: unknown event type from log: \n', line
            return None
        #get user #1
        s1,s2 = str1.split(':');
        s2 = s2.strip();
        usr1 = s2.strip("\"")
        #get hser #2 or purchase amount
        s1,item = str2.split(':');
        item = item.strip()
        if event == EV_TYPE.PURCHASE:
            item = item.strip("\"")
            value = float(item)
            usr2 = ""
        else:
            usr2 = item.strip("\"")
            value = -1.0
       
        li = log_item(event, stamp, usr1, usr2, value, line)
        return li

"""
user_db: maitain user's info, every row is a single user info
"""
class user_db(object):
    def __init__(self):
        self.db = {} #hash table {user_id, user_info}

    def add(self, ui):
        self.db[ui.id] = ui

    def check(self, id):
        if id in self.db:
            return True
        return False

"""
user_info: Containing single user's info about id, friend list, circle list, 
           and last Nth purchase
"""
class user_info(object):
    def __init__(self, id):
        self.id = id
        self.friends = Set([]) #hashset to record all user's friend
        self.circle = Set([]) #hashset to record all user's circle within N degree
        self.purchase = [] #list for all previous purchase records

"""
PurchaseAnalyze_Service:
   1) Receieve purchase log to update user_db
   2) Analysis purchase when required
"""
class PurchaseAnalyze_Service(object):
    def __init__(self, ud_obj, NPurchase, fn_out):
        self.NPurchase = NPurchase
        self.db = ud_obj.db
        self.fd_out = open(fn_out, 'w')
        
    def update(self, message):
        id = message.usr1
        val = message.value
        time = message.stamp
        plist = (self.db)[id].purchase
        plist.append([time, val])

    def analysis(self, message):
        id = message.usr1
        val = message.value
        circle = (self.db)[id].circle
        heap = [] #heap to retrieve most recent purchase
        prevPurchase = [] #used to record N previous purchase in friend circle
        heapq.heapify(heap)
        #Initialize the heap
        for cid in circle:
            plist = (self.db)[cid].purchase
            if len(plist) > 0:
                loc = len(plist) - 1
                heapq.heappush(heap, (plist[loc][0], plist[loc][1], cid, loc))
        #Get most recent N purchase
        i = 0
        while i < self.NPurchase and len(heap) > 0:
            t, v, cid, loc = heapq.heappop(heap)
            prevPurchase.append(v)
            if loc > 0:
                loc -= 1
                plist = (self.db)[cid].purchase
                heapq.heappush(heap, (plist[loc][0], plist[loc][1], cid, loc))
            i += 1
        #Compute mean and divation from previous N purchases
        if len(prevPurchase) < 2:
            return
        ava, div = self.static_compute(prevPurchase)
        benchmark = ava + 3 * div
        if val <= benchmark:
            return
        output = "{" + message.line + ", \"mean\": \""  + "{:.2f}".format(ava) + "\", "
        output += "\"sd\": \"" + "{:.2f}".format(div) + "\"}\n"
        self.fd_out.write(output)

    def static_compute(self, prevPurchase):
        n = len(prevPurchase)
        ava = 0
        for i in range(len(prevPurchase)):
            ava += prevPurchase[i]
        ava /= n
        div = 0
        for i in range(len(prevPurchase)):
            div += (prevPurchase[i] - ava) * (prevPurchase[i] - ava)
        div /= n
        div = math.sqrt(div)
        return ava, div

    def finalize(self):
        self.fd_out.close()

"""
SocialNetWork_Service: Receieve friend/unfriend info to update friend and circle info
"""
class SocialNetwork_Service(object):
    def __init__(self, ud, Ndegree):
        self.Ndegree = Ndegree
        self.ud = ud

    def process(self, message):
        id1 = message.usr1
        id2 = message.usr2
        if EV_TYPE.FRIEND == message.event:
            self.befriend(id1, id2)
        elif EV_TYPE.UNFRIEND == message.event:
            self.unfriend(id1, id2)
        else:
            self.adduser(id1)

    def adduser(self, id):
        if self.ud.check(id) == False:
            u = user_info(id)
            self.ud.add(u)

    def befriend(self, id1, id2):
        #Add new user if not existed
        self.adduser(id1)
        self.adduser(id2)
        #add each one into other's friend list
        u1 = self.ud.db[id1]
        u2 = self.ud.db[id2]
        u1.friends.add(id2)
        u2.friends.add(id1)
        #update circle for related members
        self.CircleUpdate(id1, id2)

    def unfriend(self, id1, id2):
        u1 = self.ud.db[id1]
        u2 = self.ud.db[id2]
        #add each one into other's friend list
        u1.friends.remove(id2)
        u2.friends.remove(id1)
        #update circle for related members
        self.CircleUpdate(id1, id2)

    def CircleUpdate(self, id1, id2):
        seed = Set([])
        cir1 = self.ud.db[id1].circle
        cir2 = self.ud.db[id2].circle
        for itr in cir1:
            seed.add(itr)
        for itr in cir2:
            seed.add(itr)
        seed.add(id1)
        seed.add(id2)
        for id in seed:
            self.BFS(id)

    #applying breath first search to update the Nth degree circle from a given id
    def BFS(self, id):
        output = self.ud.db[id].circle
        output.clear()
        visited = Set()
        q = Queue()
        q.put((id, 0))
        while False == q.empty():
            tid, degree = q.get()
            visited.add(tid)
            if (degree > 0):
                output.add(tid)
            if degree == self.Ndegree:
                continue
            fset = self.ud.db[tid].friends
            for fid in fset:
                if fid in visited:
                    continue
                q.put((fid, degree + 1))
        q.task_done()

"""
Return two parameters D (total degree of social network) and
T (number of tracked total purchases)
"""
def GetPar(line):
    line = line.strip()
    line = line.strip('{}')
    
    s1,s2 = line.split(',')
    # get D
    s1, str_D = s1.split(':')
    str_D = str_D.strip(' ')
    str_D = str_D.strip('\"')
    D = int(str_D)
    # get T
    s1, str_T = s2.split(':')
    str_T = str_T.strip(' ')
    str_T = str_T.strip('\"')
    T = int(str_T)
    return D,T

"""
pipeline: control the whole dataflow among parser, 
        SocialNetworkService and PurchaseAnalyze_Service
"""
def data_pipeline(line, obj_par, obj_sns, obj_pas, analysis_flg, log_output):
    item = obj_parser.process(line)
    if (True == log_output):
        item.print_out() #debug output
    #Update social network
    obj_sns.process(item)
    #Otherwise, update purchase
    if EV_TYPE.PURCHASE == item.event:
        obj_pas.update(item)
        if analysis_flg == True:
            obj_pas.analysis(item)

"""
main program
"""
batch_log = sys.argv[1]
stream_log = sys.argv[2]
output = sys.argv[3]
print 'input batch log is ', batch_log
print 'input stream log is ', stream_log
print 'output result is ', output

obj_parser = Parser()
ud_obj = user_db()

f_batch = open(batch_log)
print 'Start to process all batch logs ...'
cnt = 0
#round 1, use batch file to build graph and user purchase history
for line in f_batch:
    if cnt == 0:
        D,T = GetPar(line)
        obj_sns = SocialNetwork_Service(ud_obj, D)
        obj_pas = PurchaseAnalyze_Service(ud_obj, T, output)
    else:
        data_pipeline(line, obj_parser, obj_sns, obj_pas, False, True)
    cnt = cnt + 1
    if cnt % 10000 == 0:
        print cnt, " batch logs has been processed..."

f_batch.close()
print 'Finish processing all batch logs ...'

#round 2, use stream file an analysis anomaly purchase
f_stream = open(stream_log)
cnt = 0
print 'Start to process all stream logs ...'
for line in f_stream:
    data_pipeline(line, obj_parser, obj_sns, obj_pas, True, True)
    cnt = cnt + 1
    if cnt % 10000 == 0:
        print cnt, " stream logs has been processed..."
print 'Finish process all stream logs ...'
f_stream.close()
obj_pas.finalize()
print 'All finished.'
