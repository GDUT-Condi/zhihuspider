# coding=utf8
import urllib2,urllib
from lxml import etree
import jsonpath
import json
import re
import redis
import pymongo
import time
#aclii处理编码问题,'ascii' codec can't decode byte 0xe8 in position 0: ordinal not in range(128)
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import random
from threading import Thread,Lock
from multiprocessing import	Process,Queue

class zhihu_full():
    def __init__(self,num):
        self.url = 'https://www.zhihu.com'
        self.proxy_list = [
            {"https": "112.74.94.142:3128"},
            {"https": "183.30.197.8:9797"},
        ]

        self.num = num

    def my_opener_login(self):
        httpsproxy_handler = urllib2.ProxyHandler() #参数random.choice(self.proxy_list)
        opener = urllib2.build_opener(httpsproxy_handler)
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
            'Cookie': 'q_c1=d1dc3d0924df403f881130e4f95f30ad|1520215343000|1520215343000; _zap=6c5ddb9e-e372-48aa-9a9a-53af9092ee81; _xsrf=0f40eb2c05cb329c8b490d62fa174c3b; d_c0="AOBsjE5rPQ2PTud-gcrXUq0w9bgl3wVhXD8=|1520240069"; __utmv=51854390.100--|2=registration_date=20160303=1^3=entry_date=20160303=1; capsion_ticket="2|1:0|10:1520269385|14:capsion_ticket|44:MGFiNDBjYThiMjM2NGJkNjk4NjRkZDY0ZmZlNDJhZjg=|3afeb1fbd55204a95facf8713913c63f03f71082bed5707e9919f729dcf14995"; z_c0="2|1:0|10:1520269398|4:z_c0|92:Mi4xRU5TMEFnQUFBQUFBNEd5TVRtczlEU1lBQUFCZ0FsVk5Wc2FLV3dDR3RJTkdSb3ZuZWxNeUN5TFZKa0V2MTdqUmRB|baa2dfcd6c9c949b372028b9d962a977374f36adee18c14c6727988b3546a0c2"; __utma=51854390.1689567540.1520240071.1520260789.1520270805.4; __utmz=51854390.1520270805.4.4.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; aliyungf_tc=AQAAAPUlgEwA1AsAVbHseH+HzXzI4L+x'
        }
        request = urllib2.Request(self.url, headers=headers)
        response = opener.open(request)
        # return response
        print '正在解析login页面'
        # response = zhihu_full().my_opener_login()
        html = response.read()
        selector = etree.HTML(html)
        title_author, titles, url_links = [], [], []
        login_data = []
        a = selector.xpath(
            '//div[@data-reactid="81"]/div[position()>0]//div[@class="ContentItem AnswerItem"]/@data-zop')
        for i in a:
            i = i.encode('utf-8')
            i = json.loads(i)
            title_author.append(i["title"])
            titles.append(i["authorName"])
        urls = selector.xpath(
            '//div[@data-reactid="81"]/div[position()>0]//a[@data-za-detail-view-element_name="Title"]/@href')
        for url_link in urls:
            try:
                url_link = re.match(r'/question/(.*?)/', url_link).group(1)
                url_link = "/question/{}".format(url_link)
                url_links.append(url_link)
            except:
                continue
        login_data.append(title_author)
        login_data.append(titles)
        login_data.append(url_links)
        print '首页页面解析完成'

        try:
            r = redis.StrictRedis(host='localhost', port=6379, db=0)
        except Exception, e:
            print e.message
        print 'redis正在存储login的urls'
        for url in login_data[2]:
            r.sadd('top_urls', url)
        print 'login的urls写入完成'

        client = pymongo.MongoClient("localhost", 27017)
        db = client.test
        zhihu = db.zhihu
        print '正在将login数据写入mongodb'
        login_data = zip(login_data[0], login_data[1])
        for name, title in login_data:
            s1 = {'name': name, 'title': title}
            zhihu.insert(s1)
        print 'login数据写入mongodb完成'
        return 0

    def my_opener_ajax(self,fail=0):
        try :
            for j in range(self.num):
                after_id = 7+8*(j+fail)
                fail_number = j
                if (j+fail) < self.num:
                    httpsproxy_handler = urllib2.ProxyHandler() #参数random.choice(self.proxy_list)
                    opener = urllib2.build_opener(httpsproxy_handler)
                    ajax_url = 'https://www.zhihu.com/api/v3/feed/topstory'
                    ajax_request = {'action_feed': 'True',
                                    'limit': 8,
                                    'session_token': '4f7c607fb9961e5c614d498582a8822f',
                                    'action': 'down',
                                    'after_id': after_id,
                                    'desktop': 'true'
                                    }
                    ajax_request = urllib.urlencode(ajax_request)
                    ajax_fullurl = ajax_url + '?' + ajax_request
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
                        'Cookie': 'q_c1=d1dc3d0924df403f881130e4f95f30ad|1520215343000|1520215343000; _zap=6c5ddb9e-e372-48aa-9a9a-53af9092ee81; _xsrf=0f40eb2c05cb329c8b490d62fa174c3b; d_c0="AOBsjE5rPQ2PTud-gcrXUq0w9bgl3wVhXD8=|1520240069"; __utmv=51854390.100--|2=registration_date=20160303=1^3=entry_date=20160303=1; capsion_ticket="2|1:0|10:1520269385|14:capsion_ticket|44:MGFiNDBjYThiMjM2NGJkNjk4NjRkZDY0ZmZlNDJhZjg=|3afeb1fbd55204a95facf8713913c63f03f71082bed5707e9919f729dcf14995"; z_c0="2|1:0|10:1520269398|4:z_c0|92:Mi4xRU5TMEFnQUFBQUFBNEd5TVRtczlEU1lBQUFCZ0FsVk5Wc2FLV3dDR3RJTkdSb3ZuZWxNeUN5TFZKa0V2MTdqUmRB|baa2dfcd6c9c949b372028b9d962a977374f36adee18c14c6727988b3546a0c2"; __utma=51854390.1689567540.1520240071.1520260789.1520270805.4; __utmz=51854390.1520270805.4.4.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; aliyungf_tc=AQAAAPUlgEwA1AsAVbHseH+HzXzI4L+x',
                        'Host': 'www.zhihu.com',
                        'Referer': 'https://www.zhihu.com/',
                        'X-API-VERSION': '3.0.53',
                        'X-UDID': 'AOBsjE5rPQ2PTud-gcrXUq0w9bgl3wVhXD8=',
                        'authorization': 'Bearer 2|1:0|10:1520269398|4:z_c0|92:Mi4xRU5TMEFnQUFBQUFBNEd5TVRtczlEU1lBQUFCZ0FsVk5Wc2FLV3dDR3RJTkdSb3ZuZWxNeUN5TFZKa0V2MTdqUmRB|baa2dfcd6c9c949b372028b9d962a977374f36adee18c14c6727988b3546a0c2',
                    }
                    request = urllib2.Request(ajax_fullurl, headers=headers)
                    response = opener.open(request)
                    html = response.read()
                    html = json.loads(html)
                    # return html
                    print '正在解析ajax数据'
                    number = 0
                    authorlist = []
                    titlelist = []
                    question_ids = []
                    url_links = []
                    questions = []
                    while number < 8:
                        if jsonpath.jsonpath(html, '$..data[{}].target.type'.format(number)) == [u'answer']:
                            title = jsonpath.jsonpath(html, '$..data[{}].target.question.title'.format(number))
                            author = jsonpath.jsonpath(html, '$..data[{}].target.author.name'.format(number))
                            questions_id = jsonpath.jsonpath(html, '$..data[{}].target.question.id'.format(number))
                            authorlist.append(author)
                            titlelist.append(title)
                            question_ids.append(questions_id)
                        number += 1
                    for questions_id in question_ids:
                        for question in questions_id:
                            questions.append(question)
                    for i in range(len(questions)):
                        question = questions[i]
                        url_links.append('/question/{}'.format(question))
                    # print 'ajax数据解析完成'
                    # return (authorlist, titlelist, url_links)
                    try:
                        r = redis.StrictRedis(host='localhost', port=6379, db=0)
                        # print 'redis正在存储ajax的urls'
                        for url in url_links:
                            r.sadd('top_urls', url)
                        # print 'ajax的urls写入完成'

                        client = pymongo.MongoClient("localhost", 27017)
                        db = client.test
                        zhihu = db.zhihu
                        # print '正在将ajax数据写入mongodb'
                        ajax_data = zip(authorlist, titlelist)
                        for name, title in ajax_data:
                            s1 = {'name': name[0], 'title': title[0]}
                            zhihu.insert(s1)
                        # print 'ajax数据写入mongodb完成'
                        print '第{}页写入完成'.format(j+1)
                        time.sleep(random.random())

                    except Exception, e:
                        print e.message
                else:
                    return 0
        except:
            fail = fail_number
            print '第{}个url抓取失败，继续往下爬'.format(fail+1)
            self.my_opener_ajax(fail)

# url : https://www.zhihu.com + redis里的url
class answer_page():
    def return_url(self):
        # 默认使用0号
        try:
            r = redis.StrictRedis(host='localhost', port=6379, db=0)
        except Exception, e:
            print e.message
        try:
            url_used = r.spop('top_urls')
            if not r.sismember('top_urls_used',url_used):
                print 'redis中还有{}个url'.format(r.scard('top_urls'))
                print '已经爬取了{}个url'.format(r.scard('top_urls_used'))
                r.sadd('top_urls_used',url_used)
                return url_used
        except:
            return 0

    def answer_response(self):
        question_url = self.return_url()
        while question_url != None:
            question_number = re.match(r'/question/(\d+)',question_url).group(1)
            url = 'https://www.zhihu.com{}'.format(question_url)
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
                'Cookie': 'q_c1=d1dc3d0924df403f881130e4f95f30ad|1520215343000|1520215343000; _zap=6c5ddb9e-e372-48aa-9a9a-53af9092ee81; _xsrf=0f40eb2c05cb329c8b490d62fa174c3b; d_c0="AOBsjE5rPQ2PTud-gcrXUq0w9bgl3wVhXD8=|1520240069"; __utmv=51854390.100--|2=registration_date=20160303=1^3=entry_date=20160303=1; aliyungf_tc=AQAAAM4fSHwRTgkAVbHseMrp8X+vGySv; capsion_ticket="2|1:0|10:1520269385|14:capsion_ticket|44:MGFiNDBjYThiMjM2NGJkNjk4NjRkZDY0ZmZlNDJhZjg=|3afeb1fbd55204a95facf8713913c63f03f71082bed5707e9919f729dcf14995"; z_c0="2|1:0|10:1520269398|4:z_c0|92:Mi4xRU5TMEFnQUFBQUFBNEd5TVRtczlEU1lBQUFCZ0FsVk5Wc2FLV3dDR3RJTkdSb3ZuZWxNeUN5TFZKa0V2MTdqUmRB|baa2dfcd6c9c949b372028b9d962a977374f36adee18c14c6727988b3546a0c2"; __utma=51854390.1689567540.1520240071.1520260789.1520270805.4; __utmb=51854390.0.10.1520270805; __utmc=51854390; __utmz=51854390.1520270805.4.4.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/',
                'Host': 'www.zhihu.com',
                'Referer': url,
                'X-UDID': 'AOBsjE5rPQ2PTud-gcrXUq0w9bgl3wVhXD8=',
                'authorization': 'Bearer 2|1:0|10:1520269398|4:z_c0|92:Mi4xRU5TMEFnQUFBQUFBNEd5TVRtczlEU1lBQUFCZ0FsVk5Wc2FLV3dDR3RJTkdSb3ZuZWxNeUN5TFZKa0V2MTdqUmRB|baa2dfcd6c9c949b372028b9d962a977374f36adee18c14c6727988b3546a0c2',
            }

            request = urllib2.Request(url,headers=headers) #data=data,
            response = urllib2.urlopen(request).read()
            selector = etree.HTML(response)
            title = selector.xpath('//h1[@class="QuestionHeader-title"]/text()')
            title = title[0]
            print title
            num = selector.xpath('//h4[@class="List-headerText"]//span/text()')
            num = num[0]
            num = int(num.replace(',',''))
            print num
            #处理ajax查找最高的点赞数
            # 若从全部回答中搜，则用以下条件
            # while offset+20 < num and max_num > offset:
            #     offset += 20
            # 由于回答的顺序跟点赞数有一定相关性，可取前60条大致满足结果却能省下无数的工作
            offset = 0
            max_num = num
            max_number = 0
            while offset < 60 and max_num > offset:
                limit = 5
                sort_by = 'default'
                data = {
                    'include': 'data[*].is_normal,admin_closed_comment,reward_info,is_collapsed,annotation_action,annotation_detail,collapse_reason,is_sticky,collapsed_by,suggest_edit,comment_count,can_comment,content,editable_content,voteup_count,reshipment_settings,comment_permission,created_time,updated_time,review_info,relevant_info,question,excerpt,relationship.is_authorized,is_author,voting,is_thanked,is_nothelp,upvoted_followees;data[*].mark_infos[*].url;data[*].author.follower_count,badge[?(type=best_answerer)].topics',
                    'offset': offset,
                    'limit': limit,
                    'sort_by': sort_by
                    }
                data = urllib.urlencode(data)
                ajax_url = 'https://www.zhihu.com/api/v4/questions/{}/answers'.format(question_number) + '?' + data  #.format(question_url)
                ajax_request = urllib2.Request(ajax_url,headers=headers)
                ajax_response = urllib2.urlopen(ajax_request)
                ajax_response = ajax_response.read()
                # 把json格式字符串转换成python对象
                jsonobj = json.loads(ajax_response)
                agree_nums = jsonpath.jsonpath(jsonobj, '$.data..voteup_count')

                for agree in agree_nums:
                    if agree > max_number:
                        #取到了更大的点赞数，更新mongodb的数据
                        max_number = agree
                        max_index = agree_nums.index(max_number)
                        answer = jsonpath.jsonpath(jsonobj,'$.data[{}].content'.format(max_index))
                        answer = answer[0]
                        answer = re.sub(r'<.*?>','', answer)
                offset +=5

            answer = '该回答有{}个赞.{}'.format(max_number,answer)
            print '解析数据完成，准备存储到mongodb'
            client = pymongo.MongoClient("localhost", 27017)
            db = client.test
            topic_answer = db.zhihu_answer
            print '正在将一条数据写入mongodb'
            s1 = {'title': title, 'answer':answer}
            topic_answer.insert(s1)
            print '写入mongodb完成'
            question_url = self.return_url()
            time.sleep(random.random())
        return 0

def parse_title():
    global r
    while True:
        mutexFlag = mutex.acquire(True)
        if mutexFlag:
            if r.scard('top_urls'):
                print '正在取知乎对应的最高点赞回答'
                mutex.release()
                try:
                    answer_page().answer_response()
                    time.sleep(random.random())
                except:
                    parse_title()
            else:
                mutex.release()
                print 'redis里的url已经爬取完成'
                break

def three_threading():
    try:
        parse1 = Thread(target=parse_title)
        parse2 = Thread(target=parse_title)
        parse3 = Thread(target=parse_title)
        parse1.start()
        parse2.start()
        parse3.start()
        parse1.join()
        parse2.join()
        parse3.join()
    except Exception, e:
        print '爬虫结束'

if __name__ == '__main__':
    # 创建两个进程，一个存url,一个解析url的数据
    #	父进程创建Queue,并传给各个子进程:
    start = time.time()
    q = Queue()
    # 创建一个互斥锁,默认未上锁
    # raw_input('按任意键开始')
    # 两个线程对answer进行解析
    mutex = Lock()
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    all_url = int(raw_input('爬虫爬多少页'))
    zhihu_full(all_url).my_opener_login()
    pw = Process(target=zhihu_full(all_url).my_opener_ajax)
    pr = Process(target=three_threading)
    pw.start()
    pr.start()
    pw.join()
    pr.join()
    end = time.time()
    print end-start