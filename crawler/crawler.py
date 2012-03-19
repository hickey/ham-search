#!/usr/bin/env python

import sys,os,re
import logging
import getopt
import urllib2
from bs4 import *
from urlparse import urljoin
import pymongo
import ConfigParser
import pprint
import fileinput
import datetime
import robotparser


# Create a list of words to ignore
ignorewords={'the':1,'of':1,'to':1,'and':1,'a':1,'in':1,'is':1,'it':1}

ignoreext=['.pdf', '.doc', '.xls', '.docx', '.xlsx', '.ppt', '.pptx', '.jpg',
           '.gif', '.pcx', '.bmp', '.tif', '.tiff', '.png', '.mov', '.fls', 
           '.flv', '.zip', '.exe', '.dmg', '.iso', '.cdr', '.pkg', '.deb',
           '.rpm', '.mp3', '.mpg', '.mpeg', '.avi', '.wmf', '.wav', '.gz',
           '.tar', '.tgz', '.bz2', '.jpeg', ]


Log = logging.getLogger('com.wt0f.search.crawler')
LogHandler = logging.FileHandler('/dev/tty')
LogHandler.setLevel(logging.DEBUG)
Log.addHandler(LogHandler)
Log.setLevel(logging.DEBUG)


class crawler:
    # Initialize the crawler with the name of database
    def __init__(self, host='localhost', port=27017, database='ham_search'):
        Log.info('Opening database connection: %s:%d/%s' % (host, port, database))
        self.conn = pymongo.Connection(host=host, port=port)
        self.db = self.conn[database]
        
        # Create the collections
        #self.createCollections()
        
        
        
        
        
    # Create the database tables
    def createCollections(self): 
        self.linklist = self.db.linklist
        self.wordlist = self.db.wordlist
        self.linkinfo = self.db.linkinfo
        self.seedlist = self.db.seedlist
        
  
  
    def __del__(self):
        self.db.logout()
        self.conn.close()

    def dbcommit(self):
        self.con.commit()

    # Starting with a list of pages, do a breadth
    # first search to the given depth, indexing pages
    # as we go
    def crawl(self):
        seeds=[]
        
        # Start to pull together the first group of URLs
        uri = self.findNextURI()
        seeds.append(uri['url'])
        for url in self.collectSimilarURI(uri['url']):
            seeds.append(url)
        
        while len(seeds) > 0:
            seed = seeds.pop()
            pagesoup = self.retreivePage(seed)
            if pagesoup:
                links = self.extractLinks(pagesoup, seed)
                for link in links:
                    self.addToUrlList(link)
            
            if len(seeds) == 0:
                uri = self.findNextURI()
                seeds.append(uri['url'])
                for url in self.collectSimilarURI(uri['url']):
                    seeds.append(url)

    # 
    def findNextURI(self):
        entry = self.db.seedlist.find_one()
        if entry:
            self.db.seedlist.remove(entry['_id'])
        return entry
        
        
    def collectSimilarURI(self, url):
        site = re.sub(r'(https?:)//([^\/]+).*', r'\1\/\/\2', url)
        Log.debug('%s --> %s' % (url, site))
        entries = self.db.seedlist.find({'url': re.compile(site, re.IGNORECASE)})
        
        similar = []
        for entry in entries:
            print "found: %s" % entry['url']
            similar.append(entry['url'])
        
        Log.debug('Asked for similar to %s\nFound: %s' % (url, similar))
        return similar
        
        
    # retreive a webpage
    def retreivePage(self, url):
        Log.info('Getting %s' % url)
        try:
            xfer=urllib2.urlopen(url)
        except:
            Log.warn("Could not open %s" % url)
            return None
            #continue
            
        # Enter the time that the URL was last visited
        record = self.db.lastvisit.find_one({'url': url})
        if record:
            record['visitDate'] = datetime.datetime.now()
            self.db.lastvisit.save(record)
        else:
            self.db.lastvisit.insert({'url': url, 'visitDate': datetime.datetime.now()})
        
        soup = BeautifulSoup(xfer.read())
        return soup
        

    # extract text from page
    def extractText(self, soup):
        return soup.text
        
    
    # extract links from page
    def extractLinks(self, soup, page):
        links = []
        linkrefs = soup('a')
        for link in linkrefs:
            if ('href' in dict(link.attrs)):
                url=urljoin(page,link['href'])
                if url.find("'")!=-1: continue
                url=url.split('#')[0]  # remove fragment
                
                if url[0:4]=='http': # and not self.isindexed(url):
                    links.append(url)
        return links
        
        
    # add a link to the urllist collection
    def addToUrlList(self, link, priority=0):
        # check to see if link is already on the list
        if self.db.seedlist.find_one({'url': link}):
            return
            
        # things to ignore
        for ext in ignoreext:
            if link.endswith(ext):
                return
                
        Log.info('Adding to seeder list: %s' % link)
        self.db.seedlist.insert({'url': link, 
                                 'foundDate': datetime.datetime.now(),
                                 'priority': priority})
        
##  # Auxilliary function for getting an entry id and adding 
##  # it if it's not present
##  def getentryid(self,table,field,value,createnew=True):
##    cur=self.con.execute(
##    "select rowid from %s where %s='%s'" % (table,field,value))
##    res=cur.fetchone()
##    if res==None:
##      cur=self.con.execute(
##      "insert into %s (%s) values ('%s')" % (table,field,value))
##      return cur.lastrowid
##    else:
##      return res[0] 


##  # Index an individual page
##  def addtoindex(self,url,soup):
##    if self.isindexed(url): return
##    print 'Indexing '+url
##  
##    # Get the individual words
##    text=self.gettextonly(soup)
##    words=self.separatewords(text)
##    
##    # Get the URL id
##    urlid=self.getentryid('urllist','url',url)
##    
##    # Link each word to this url
##    for i in range(len(words)):
##      word=words[i]
##      if word in ignorewords: continue
##      wordid=self.getentryid('wordlist','word',word)
##      self.con.execute("insert into wordlocation(urlid,wordid,location) values (%d,%d,%d)" % (urlid,wordid,i))
  

  
##  # Extract the text from an HTML page (no tags)
##  def gettextonly(self,soup):
##    v=soup.string
##    if v==Null:   
##      c=soup.contents
##      resulttext=''
##      for t in c:
##        subtext=self.gettextonly(t)
##        resulttext+=subtext+'\n'
##      return resulttext
##    else:
##      return v.strip()
##
##  # Seperate the words by any non-whitespace character
##  def separatewords(self,text):
##    splitter=re.compile('\\W*')
##    return [s.lower() for s in splitter.split(text) if s!='']
##
##    
##  # Return true if this url is already indexed
##  def isindexed(self,url):
##    return False
##  
##  # Add a link between two pages
##  def addlinkref(self,urlFrom,urlTo,linkText):
##    words=self.separateWords(linkText)
##    fromid=self.getentryid('urllist','url',urlFrom)
##    toid=self.getentryid('urllist','url',urlTo)
##    if fromid==toid: return
##    cur=self.con.execute("insert into link(fromid,toid) values (%d,%d)" % (fromid,toid))
##    linkid=cur.lastrowid
##    for word in words:
##      if word in ignorewords: continue
##      wordid=self.getentryid('wordlist','word',word)
##      self.con.execute("insert into linkwords(linkid,wordid) values (%d,%d)" % (linkid,wordid))
##

##  def calculatepagerank(self,iterations=20):
##    # clear out the current page rank tables
##    self.con.execute('drop table if exists pagerank')
##    self.con.execute('create table pagerank(urlid primary key,score)')
##    
##    # initialize every url with a page rank of 1
##    for (urlid,) in self.con.execute('select rowid from urllist'):
##      self.con.execute('insert into pagerank(urlid,score) values (%d,1.0)' % urlid)
##    self.dbcommit()
##    
##    for i in range(iterations):
##      print "Iteration %d" % (i)
##      for (urlid,) in self.con.execute('select rowid from urllist'):
##        pr=0.15
##        
##        # Loop through all the pages that link to this one
##        for (linker,) in self.con.execute(
##        'select distinct fromid from link where toid=%d' % urlid):
##          # Get the page rank of the linker
##          linkingpr=self.con.execute(
##          'select score from pagerank where urlid=%d' % linker).fetchone()[0]
##
##          # Get the total number of links from the linker
##          linkingcount=self.con.execute(
##          'select count(*) from link where fromid=%d' % linker).fetchone()[0]
##          pr+=0.85*(linkingpr/linkingcount)
##        self.con.execute(
##        'update pagerank set score=%f where urlid=%d' % (pr,urlid))
##      self.dbcommit()

##class searcher:
##  def __init__(self,dbname):
##    self.con=sqlite.connect(dbname)
##
##  def __del__(self):
##    self.con.close()
##
##  def getmatchrows(self,q):
##    # Strings to build the query
##    fieldlist='w0.urlid'
##    tablelist=''  
##    clauselist=''
##    wordids=[]
##
##    # Split the words by spaces
##    words=q.split(' ')  
##    tablenumber=0
##
##    for word in words:
##      # Get the word ID
##      wordrow=self.con.execute(
##      "select rowid from wordlist where word='%s'" % word).fetchone()
##      if wordrow!=None:
##        wordid=wordrow[0]
##        wordids.append(wordid)
##        if tablenumber>0:
##          tablelist+=','
##          clauselist+=' and '
##          clauselist+='w%d.urlid=w%d.urlid and ' % (tablenumber-1,tablenumber)
##        fieldlist+=',w%d.location' % tablenumber
##        tablelist+='wordlocation w%d' % tablenumber      
##        clauselist+='w%d.wordid=%d' % (tablenumber,wordid)
##        tablenumber+=1
##
##    # Create the query from the separate parts
##    fullquery='select %s from %s where %s' % (fieldlist,tablelist,clauselist)
##    print fullquery
##    cur=self.con.execute(fullquery)
##    rows=[row for row in cur]
##
##    return rows,wordids
##
##  def getscoredlist(self,rows,wordids):
##    totalscores=dict([(row[0],0) for row in rows])
##
##    # This is where we'll put our scoring functions
##    weights=[(1.0,self.locationscore(rows)), 
##             (1.0,self.frequencyscore(rows)),
##             (1.0,self.pagerankscore(rows)),
##             (1.0,self.linktextscore(rows,wordids)),
##             (5.0,self.nnscore(rows,wordids))]
##    for (weight,scores) in weights:
##      for url in totalscores:
##        totalscores[url]+=weight*scores[url]
##
##    return totalscores
##
##  def geturlname(self,id):
##    return self.con.execute(
##    "select url from urllist where rowid=%d" % id).fetchone()[0]
##
##  def query(self,q):
##    rows,wordids=self.getmatchrows(q)
##    scores=self.getscoredlist(rows,wordids)
##    rankedscores=[(score,url) for (url,score) in scores.items()]
##    rankedscores.sort()
##    rankedscores.reverse()
##    for (score,urlid) in rankedscores[0:10]:
##      print '%f\t%s' % (score,self.geturlname(urlid))
##    return wordids,[r[1] for r in rankedscores[0:10]]
##
##  def normalizescores(self,scores,smallIsBetter=0):
##    vsmall=0.00001 # Avoid division by zero errors
##    if smallIsBetter:
##      minscore=min(scores.values())
##      return dict([(u,float(minscore)/max(vsmall,l)) for (u,l) in scores.items()])
##    else:
##      maxscore=max(scores.values())
##      if maxscore==0: maxscore=vsmall
##      return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])
##
##  def frequencyscore(self,rows):
##    counts=dict([(row[0],0) for row in rows])
##    for row in rows: counts[row[0]]+=1
##    return self.normalizescores(counts)
##
##  def locationscore(self,rows):
##    locations=dict([(row[0],1000000) for row in rows])
##    for row in rows:
##      loc=sum(row[1:])
##      if loc<locations[row[0]]: locations[row[0]]=loc
##    
##    return self.normalizescores(locations,smallIsBetter=1)
##
##  def distancescore(self,rows):
##    # If there's only one word, everyone wins!
##    if len(rows[0])<=2: return dict([(row[0],1.0) for row in rows])
##
##    # Initialize the dictionary with large values
##    mindistance=dict([(row[0],1000000) for row in rows])
##
##    for row in rows:
##      dist=sum([abs(row[i]-row[i-1]) for i in range(2,len(row))])
##      if dist<mindistance[row[0]]: mindistance[row[0]]=dist
##    return self.normalizescores(mindistance,smallIsBetter=1)
##
##  def inboundlinkscore(self,rows):
##    uniqueurls=dict([(row[0],1) for row in rows])
##    inboundcount=dict([(u,self.con.execute('select count(*) from link where toid=%d' % u).fetchone()[0]) for u in uniqueurls])   
##    return self.normalizescores(inboundcount)
##
##  def linktextscore(self,rows,wordids):
##    linkscores=dict([(row[0],0) for row in rows])
##    for wordid in wordids:
##      cur=self.con.execute('select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.rowid' % wordid)
##      for (fromid,toid) in cur:
##        if toid in linkscores:
##          pr=self.con.execute('select score from pagerank where urlid=%d' % fromid).fetchone()[0]
##          linkscores[toid]+=pr
##    maxscore=max(linkscores.values())
##    normalizedscores=dict([(u,float(l)/maxscore) for (u,l) in linkscores.items()])
##    return normalizedscores
##
##  def pagerankscore(self,rows):
##    pageranks=dict([(row[0],self.con.execute('select score from pagerank where urlid=%d' % row[0]).fetchone()[0]) for row in rows])
##    maxrank=max(pageranks.values())
##    normalizedscores=dict([(u,float(l)/maxrank) for (u,l) in pageranks.items()])
##    return normalizedscores
##
##  def nnscore(self,rows,wordids):
##    # Get unique URL IDs as an ordered list
##    urlids=[urlid for urlid in dict([(row[0],1) for row in rows])]
##    nnres=mynet.getresult(wordids,urlids)
##    scores=dict([(urlids[i],nnres[i]) for i in range(len(urlids))])
##    return self.normalizescores(scores)
##


if __name__ == '__main__':
    Option = {}
    config = ConfigParser.ConfigParser()
    
    opts, args = getopt.getopt(sys.argv[1:], 'dvc:s:Dx', 
                        ['debug', 'verbose', 'config=', 'seedfile=',
                         'delete-seedlist'])
    for opt, val in opts:
        if opt == '-v' or opt =='--verbose':
            Option['verbose'] = True
            
        if opt == '-d' or opt == '--debug':
            Option['debug'] = True
            
        if opt == '-c' or opt == '--config':
            config.read(val)
            
        if opt == '-s' or opt == '--seedfile':
            Option['seedfile'] = val
            
        if opt == '-D' or opt == '--delete-seedlist':
            Option['delete_seed'] = True
            
        if opt == '-x':
            Option['execute'] = True
            
    
    crawl = crawler(host=config.get('database', 'host'), 
                    port=int(config.get('database', 'port')),
                    database=config.get('database', 'name'))
    

    if Option.has_key('delete_seed'):
        while crawl.findNextURI():
            sys.stdout.write ('.')
        
            
    # preload the seed list
    if Option.has_key('seedfile'):
        try:
            for line in fileinput.input(Option['seedfile']):
                crawl.addToUrlList(line.strip())
        except IOError, e:
            Log.error('Unable to read seedfile: %s', Option['seedfile'])
        
        
    if Option.has_key('execute'):
        crawl.crawl()
        