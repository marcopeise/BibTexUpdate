# generic imports
import scrapy
import bibtexparser
import time
import datetime

# imports for exporting bibtex
from bibtexparser.bwriter import BibTexWriter

# Error Handling imports
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

# imports for queue Handling
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

class SpringerSpider(scrapy.Spider):
    name = "springer"

    # global variables
    idx = 0
    bib_databaselength = 0
    bib_databaseentries = []
    parsingidx = 0
    abstractnotfound = 0
    keywordsnotfound = 0
    doinotfound = 0
    start_time = time.time()
    bib_databaseentriesnotfound = []

    # ==================================================
    # IMPORT BibTex file
    # ==================================================
    # TODO: Filename as Input Variable:
    print("====================")
    print("parsing InputBibTex.bib ...")
    print("====================")
    with open('InputBibTex.bib') as bibtex_file:
        bib_database = bibtexparser.bparser.BibTexParser(common_strings=True).parse_file(bibtex_file)

    # ==================================================
    # SCRAPE INIT Function
    # ==================================================
    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)


    # ==================================================
    # REQUEST function
    # ==================================================
    # for every bibtex entry one http call will be made

    def start_requests(self):
        # TODO: restructure to global
        self.bib_databaselength = len(self.bib_database.entries)
        self.bib_databaseentries = self.bib_database.entries
        print("====================")
        print("calling URLs for %s" % self.bib_databaselength + " entries ...")
        print("====================")
        for self.idx, entry in enumerate(self.bib_database.entries):
          print("====================")
          loggingidx= self.idx +1
          print("scraping %s" % loggingidx + " of %s" % self.bib_databaselength + " entries ... ")
          print("====================")
          yield scrapy.Request(entry["url"], callback=self.parse_httpbin,
                                    errback=self.errback_httpbin,
                                    dont_filter=True)


    # ==================================================
    # HTTP Parser function
    # ==================================================
    # for every http scrape these tasks are performed

    def parse_httpbin(self, response):
        self.logger.info('Got successful response from {}'.format(response.url))

        if self.idx==1:
            self.parsingidx = self.idx - 1
        entry = self.bib_databaseentries[self.parsingidx]

        # simple test if abstract, keywords and doi exist on the crawled HTML side ?
        abstractExists = response.xpath('//section[@id="Abs1"]/p/text()').extract_first(default='Abstract not-found')
        keywordsExists = response.xpath('//div[@class="KeywordGroup"]/span[@class="Keyword"]/text()').extract_first(default='Keywords not-found')
        doiExists = response.xpath("//meta[@name='citation_doi']/@content").extract_first(default='Citation DOI not-found')

        # ==================================================
        # XPATH for abstract #1
        # ==================================================
        # because of other HTML structure, some entries need other xpaths
        if ("Abstract not-found" in abstractExists):
            abstractExists = response.xpath('//div[@id="Abstract"]/p/text()').extract_first(default='Abstract not-found')

        finalabstract = ""
        #print("abstractExists ", abstractExists)

        # ==================================================
        # XPATH for abstract #2
        # ==================================================
        # because of other HTML structure, some entries need other xpaths
        if ("Abstract not-found" not in abstractExists):
            abstractsection = response.xpath('//section[@id="Abs1"]')

            # loop through many paragraph seperated elements of the abstract
            for p in abstractsection.xpath('.//p/text()'):
                #print p.extract()
                finalabstract = finalabstract + ' ' + p.extract()

        # if other XPATHs did not get the abstract, it is marked as "Abstract not-found"
        else:
            finalabstract = abstractExists

        finalkeywords = ""
        #print("keywordsExists ", keywordsExists)
        if ("Keywords not-found" in keywordsExists):
            keywordgroupexists = response.xpath('//div[@id="Keywords"]/ul/li[@class="c-keywords__item"]/text()').extract_first(default='Keywords not-found')
            #print(keywordgroupexists)
            if ("Keywords not-found" not in keywordgroupexists):
                keywordgroup = response.xpath('//div[@id="Keywords"]/ul/li[@class="c-keywords__item"]/text()')
                for keyword in keywordgroup:
                    #print(keyword.extract().rstrip())
                    finalkeywords = finalkeywords.rstrip() + ", " + keyword.extract().rstrip()
            else:
                finalkeywords = keywordgroupexists
        else:
            keywordgroup = response.xpath('//div[@class="KeywordGroup"]')
            for keyword in keywordgroup.xpath('.//span[@class="Keyword"]/text()'):
                finalkeywords = finalkeywords.rstrip() + ", " + keyword.extract().rstrip()

        #yield {
        #  'doi': doiExists,
        #  'keywords': finalkeywords,
        #  'abstract': finalabstract,
        #}

        # ==================================================
        # OUTPUT
        # ==================================================
        # adding abstract and keywords to the entry

        entry["abstract"] = finalabstract
        entry["keywords"] = finalkeywords

        # ==================================================
        # Error Logging & Preparation for Error File
        # ==================================================
        # distinguish between abstract, keywords or DOI not foundry

        if(finalabstract == 'Abstract not-found'):
            self.abstractnotfound += 1
            #entry["abstractnotfound"]=1
        if(finalkeywords == 'Keywords not-found'):
            #testing other location on website
            #//ul[@class="c-keywords"]
            self.keywordsnotfound += 1
            #entry["keywordsnotfound"]=1
        if(doiExists == 'Citation DOI not-found'):
            self.doinotfound += 1
            #entry["doinotfound"]=1
        if( (finalabstract =='Abstract not-found') or (finalkeywords=='Keywords not-found') or (doiExists == 'Citation DOI not-found')):
            entrynotfound = entry.copy()
            print("====================")
            print("       NOT FOUND    ")
            print("====================")
            print("finalabstract", finalabstract)
            print("finalkeywords", finalkeywords)
            print("doiExists", doiExists)
            if (finalabstract =='Abstract not-found'):
                    entrynotfound["abstractnotfound"] = 1
            if (finalkeywords =='Keywords not-found'):
                    entrynotfound["keywordsnotfound"] = 1
            if (doiExists =='Citation DOI not-found'):
                    entrynotfound["doinotfound"] = 1
            print(entrynotfound)
            self.bib_databaseentriesnotfound.append(entrynotfound)

        self.parsingidx +=1

    # ==================================================
    # SCRAPY CLOSE function
    # ==================================================
    # when no further process within scrapy is performed, this function will be called

    def spider_closed(self, spider):
        print("closing scrapy")

        # ==================================================
        # EXPORT to BibTex File
        # ==================================================
        # TODO: Filename as Input Variable
        with open('OutputBibTex.bib', 'w') as bibtex_file:
            bibtexparser.dump(self.bib_database, bibtex_file)

        # tracking time
        elapsed_time = time.time() - self.start_time
        m, s = divmod(elapsed_time, 60)
        h, m = divmod(m, 60)

        #print report
        print("====================")
        print("       Report")
        print("====================")
        print("amount of parsed bibtex entries: %s" % self.bib_databaselength)
        print("amount of time elapsed: %d:%02d:%02d" % (h, m, s))
        print("amount of entries where abstract could NOT be found: %s" % self.abstractnotfound)
        print("amount of entries where keywords could NOT be found: %s" % self.keywordsnotfound)
        print("amount of entries where DOI could NOT be found: %s" % self.doinotfound)

        # ==================================================
        # EXPORT Crawl Error File
        # ==================================================
        # TODO: Filename as Input Variable
        print("creating not-found file BibtextEntriesNotFound.txt ...")
        with open('BibtextEntriesNotFound.txt', 'w') as f:
            for entry in self.bib_databaseentriesnotfound:
                #print(entry)
                filestring = entry["ID"] + " , " + entry["url"]
                if ("doinotfound" in entry) and (entry["doinotfound"] == 1):
                    filestring = filestring + " , " + 'DOI not found'
                if ("keywordsnotfound" in entry) and (entry["keywordsnotfound"] == 1):
                    print("keyword missing")
                    filestring = filestring + " , " + entry["keywords"]
                if ("abstractnotfound" in entry) and (entry["abstractnotfound"] == 1):
                    print("abstract missing")
                    filestring = filestring + " , " + entry["abstract"]
                f.write("%s\n" % filestring)


    # ==================================================
    # HTTP error handling function
    # ==================================================

    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)
