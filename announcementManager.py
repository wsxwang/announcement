#!/usr/bin/python
#-*-coding:utf-8-*-

"""
announcementManager.py
"""

import os
import sys
import signal
import re
import time
import inspect
from ftplib import FTP
from xml.etree import ElementTree
from xml.dom import minidom
import base64
import traceback
import logging
import logging.handlers
import socket


#-------------------------------------------------
"""
push file to ftp
filePath:abs dir and file name
host: (id:ip:port:user:pwd)
return: True or False
"""
def pushFile(filePath, host):
    if not os.path.exists(filePath):
        return False;

    #wait 1 minute, while client may uploading this file
    filemtime = os.path.getmtime(filePath);
    tmNow = time.localtime();
    if (time.mktime(tmNow) - filemtime) < 60:
        return False;
    
    global g_ftpServerOK;
    if host not in g_ftpServerOK:
        return False;
    
    annRoot = "/files";

    seps = str.split(host, ":");
    baseName = os.path.basename(filePath);
    annName =  baseName[0:20];
    loginOK = False;

    try:
        #connect to ftp server
        ftp = FTP();
        ftp.connect(seps[1], seps[2]);
        ftp.login(seps[3], seps[4]);
        logging.debug("connect ok," + host);
        loginOK = True;

        #annRoot must create first
        dirs = ftp.nlst("/");
        if annRoot not in dirs:
            ftp.mkd(annRoot);
            logging.debug("mkdir," + host + annRoot);
        ftp.cwd(annRoot);

        #only create announcement dir while filePath is description xml file
        dirs = ftp.nlst();
        if annName not in dirs:
            if baseName.endswith(".xml"):
                ftp.mkd(annName);
                logging.debug("mkdir," + host + annRoot + "/" + annName);
            else:
                return False;
        ftp.cwd(annName);

        #upload file
        ftp.storbinary("STOR " + baseName, open(filePath, "rb"));

        #report state
        genAnnouncementState(filePath, host);

        logging.info("push ok," + seps[0] + ":" + seps[1] + ":" + seps[2] + "," + baseName);
    except Exception , e:
        logging.warn("ftp fail," + annName + "-->" + host + "," + str(e) + "," + traceback.format_exc());
    finally:
        if loginOK:
            ftp.quit();

    return True;

"""
YYYYmmdd_xxxxxx_xxxx
"""
def isAnnName(n):
    return re.match("\d{8}_\d{6}_\d{4}", n);

"""
100000
"""
def isRootID(nodeID):
    return nodeID == "100000";

"""
xx0000
"""
def isProvinceID(nodeID):
    return re.match("\d{2}0000", nodeID) and nodeID != "100000";

"""
last 4 number is not 0000
"""
def isCityID(nodeID):
    return (len(nodeID) == 6) and (not isRootID(nodeID)) and (not isProvinceID(nodeID));

"""
root has no upnode
upnode of provice is root
upnode of city is provice
"""
def getUpNodeID(nodeID):
    if isRootID(nodeID):
        return "";
    if isProvinceID(nodeID):
        return "100000";
    if isCityID(nodeID):
        return nodeID[0:2] + "0000";
    return "";

"""
parse announcement description xml file
return ftp server list, arrary node like (id:ip:port:usr:pwd)
"""
def parseXml4Nodes(xmlPath):
    nodes = [];
    if os.path.exists(xmlPath) and os.path.isfile(xmlPath):
        try:
            xmlDoc = ElementTree.parse(xmlPath)
            xmlRoot = xmlDoc.getroot()
            propertyFrom = xmlRoot.find("property[@name='from']");
            fromID = propertyFrom.attrib["value"];
            propertyTo = xmlRoot.findall("property[@name='to']");
            for i in propertyTo:
                hostID = i.attrib["value"];
                if hostID == fromID:
                    continue;
                host = g_config.getFtpCfg(hostID);
                if host == "::::":
                    logging.warn(os.path.basename(xmlPath)[0:20] + " need push to " + hostID + ",not config");
                else:
                    nodes.append(host);
        except Exception , e:
            logging.warn("xml fail," + xmlPath + "," + str(e) + "," + traceback.format_exc());
    return nodes;

"""
choose where to push announcement
1.node must direct linked
2.xml and rtf file not pushed
annName:announcement name
nodes:ftp server list, node is (id:ip:port:usr:pwd)
return:same as nodes, need push to these nodes
"""
def chooseNodes4Push(annName, nodes):
    pushNodes = [];
    directLinkNodes = [];
    directLinkNodes = chooseNodesDirectLink(nodes);
    for n in directLinkNodes:
        seps = str.split(n, ":");
        xmlStatePath =  os.path.join(g_config.m_localFtpRoot, "state", annName, annName + ".xml." + seps[0] + ".xml");
        rtfStatePath =  os.path.join(g_config.m_localFtpRoot, "state", annName, annName + ".rtf." + seps[0] + ".xml");
        if os.path.exists(xmlStatePath) and os.path.exists(rtfStatePath):
            continue;
        pushNodes.append(n);
    return pushNodes;

"""
choose ftp server direct link to local node
nodes:ftp server list,node is (id:ip:port:usr:pwd)
return:same as nodes,exclude those not direct linked
"""
def chooseNodesDirectLink(nodes):
    global g_config;
    pushNodes = [];
    localID = g_config.m_localNodeID;
    if localID == "":
        return pushNodes;
    for n in nodes:
        seps = str.split(n, ":");
        if seps[0] == localID:
            continue;
        if isRootID(localID):
            if isProvinceID(seps[0]):
                pushNodes.append(n);
        elif isProvinceID(localID):
            if isRootID(seps[0]) or (getUpNodeID(seps[0]) == localID):
                pushNodes.append(n);
        elif isCityID(localID):
            if getUpNodeID(localID) == seps[0]:
                pushNodes.append(n);
                break;
    return pushNodes;

"""
create file pushed report file
filePath:file pushed(abs dir and file name)
host:(id:ip:port:user:pwd)
"""
def genAnnouncementState(filePath, host):
    fileName = os.path.basename(filePath);
    annName =  fileName[0:20];
    stateDir = os.path.join(g_config.m_localFtpRoot, "state", annName);
    if not os.path.exists(stateDir):
        os.makedirs(stateDir);
    seps = str.split(host, ":");
    statePath = os.path.join(stateDir, fileName + "." + seps[0] + ".xml");
    
    doc = minidom.Document();
    rootNode = doc.createElement("announcementState");
    doc.appendChild(rootNode);
    propertyNode = doc.createElement("property");
    propertyNode.setAttribute("key", "file");
    propertyNode.setAttribute("value", fileName);
    rootNode.appendChild(propertyNode);
    propertyNode = doc.createElement("property");
    propertyNode.setAttribute("key", "to");
    propertyNode.setAttribute("value", seps[0]);
    rootNode.appendChild(propertyNode);
    propertyNode = doc.createElement("property");
    propertyNode.setAttribute("key", "sendTime");
    propertyNode.setAttribute("value", time.strftime("%Y-%m-%d %H:%M:%S"));
    rootNode.appendChild(propertyNode);
    
    f = file(statePath, "w");
    doc.writexml(f, encoding="utf-8");
    f.close();

"""
delete path
if path is directory, delete all files and directory in it
"""
def rmAllDirandFiles(path):
    time.sleep(1);
    if not os.path.exists(path):
        return;
    if os.path.isdir(path):
        for f in os.listdir(os.path.normpath(path)):
            rmAllDirandFiles(os.path.join(path,f));
        os.rmdir(path);
        logging.info("rmdir " + path);
    else:
        os.remove(path);
        logging.info("remove " + path);

#-------------------------------------------------
"""
not complete disk
clear old xml,rtf,att,state files and directory
"""
def autoClear():
    global g_config;
    localPath = os.path.normpath(g_config.m_localFtpRoot + "/files")
    statePath = os.path.normpath(g_config.m_localFtpRoot + "/state")
    if os.path.exists(localPath) and os.path.isdir(localPath):
        for i in os.listdir(os.path.normpath(localPath)):
            if isAnnName(i):
                annPath = os.path.join(localPath, i)
                if os.path.isdir(annPath):
                    tmFile = time.strptime(i[0:8], "%Y%m%d");
                    tmNow = time.localtime();
                    timeDelay = (time.mktime(tmNow) - time.mktime(tmFile)) / 3600 / 24;
                    if(timeDelay > g_config.m_autoclearXmlDelay):
                        #dir and state
                        rmAllDirandFiles(annPath);
                        rmAllDirandFiles(os.path.join(statePath, i));
                        continue;
                    if(timeDelay > g_config.m_autoclearRtfDelay):
                        #rtf
                        rtfPath = os.path.join(annPath, i + ".rtf");
                        rmAllDirandFiles(rtfPath);
                    if(timeDelay > g_config.m_autoclearAttDelay):
                        #atts
                        for f in annPath:
                            if re.match("*.att", f):
                                rmAllDirandFiles(f);

#-------------------------------------------------
"""
push xml and rtf
"""
def pushAnn():
    global g_flagTerm;
    localPath = os.path.normpath(g_config.m_localFtpRoot + "/files")
    if os.path.exists(localPath) and os.path.isdir(localPath):
        for i in os.listdir(os.path.normpath(localPath)):
            if isAnnName(i):
                annPath = os.path.join(localPath, i)
                if os.path.isdir(annPath):
                    logging.debug("work on " + i)
                    xmlPath = os.path.join(annPath, i + ".xml");
                    rtfPath = os.path.join(annPath, i + ".rtf");
                    nodes = parseXml4Nodes(xmlPath);
                    nodes = chooseNodes4Push(i, nodes);
                    if len(nodes) > 0:
                        logging.debug(i + " need push to " + str(nodes));
                    for node in nodes:
                        if g_flagTerm == True:
                            break;
                        pushFile(node, xmlPath);
                        pushFile(node, rtfPath);
                        
"""

not complete

"""
def pushAtt():
    pass;

"""
ftp server connection state is ok.
array node like(id:ip:port:usr:pwd)
"""
g_ftpServerOK = [];

"""
check ftp server connection state
only check direct link server
write to $(local ftp root)/state/connectionState.xml
"""
def checkConnectionState():
    global g_config;
    global g_ftpServerOK;
    g_ftpServerOK = [];
    hostStates = {}
    
    nodes = chooseNodesDirectLink(g_config.ftpCfgMap.values());
    for node in nodes:
        seps = str.split(node, ":");
        loginOK = False;
        try:
            ftp = FTP();
            ftp.connect(seps[1], seps[2]);
            ftp.login(seps[3], seps[4]);
            logging.debug("server ok," + seps[0] + ":" + seps[1] + ":");
            g_ftpServerOK.append(node);
            loginOK = True;
        except Exception , e:
            logging.warn("server fail," + seps[0] + ":" + seps[1] + ":");
        finally:
            if loginOK:
                ftp.quit();
        hostStates[node] = loginOK;
    genConnectionState(hostStates);

"""

ftp server connection state
hostStates:directory,key(id:ip:port:usr:pwd),value(True/False)
"""
def genConnectionState(hostStates):
    global g_config;
    stateDir = os.path.join(g_config.m_localFtpRoot, "state");
    if not os.path.exists(stateDir):
        os.makedirs(stateDir);
    filePath = os.path.join(stateDir, "connectionState.xml");
    
    doc = minidom.Document();
    rootNode = doc.createElement("connectionState");
    doc.appendChild(rootNode);
    for host, state in hostStates.items():
        seps = str.split(host, ":");
        connectionNode = doc.createElement("connection");
        connectionNode.setAttribute("id", seps[0]);
        connectionNode.setAttribute("host", seps[1]);
        if state:    
            connectionNode.setAttribute("state", "succ");
        else:
            connectionNode.setAttribute("state", "fail");
        rootNode.appendChild(connectionNode);
    f = file(filePath, "w");
    doc.writexml(f, encoding="utf-8");
    f.close();
    
#------------------------------------------------
#config
class Config:
    """
    parse announcementManager.xml,ftpConfig.xml    
    """
    def parse(self, cfgPath = "/u01/etc/announcementManager"):
        print "config file path is " + cfgPath;
        try:
            self.parseProgramCfg(os.path.normpath(cfgPath) + "/announcementManager.xml");
            self.parseFtpCfg(os.path.normpath(cfgPath) + "/ftpConfig.xml");
            self.parseLocalNodeID();
        except Exception , e:
            print "parse config fail," + cfgPath + "," + str(e) + "," + traceback.format_exc();    
    
    m_localFtpRoot = "/u01/ftproot";      #local ftp root dir, not complete

    m_logPath = "/u01/log/announcementManager";
    m_logLevel = "info";

    m_autoclearDiskPercent = 80;  #disk usage,default 80%
    m_autoclearXmlDelay = 365;    #xml files clear after 365 days
    m_autoclearRtfDelay = 365;    #rtf files clear after 365 days
    m_autoclearAttDelay = 7;      #att files clear after 7 days
    
    """
    parse announcementManager.xml
    not complete  disk
    """
    def parseProgramCfg(self, fileName):
        if not os.path.exists(fileName):
            return;
        print "parse " + fileName;
        xmlDoc = ElementTree.parse(fileName)
        xmlRoot = xmlDoc.getroot()
        #/announcementManager/localCfg/log
        item = xmlRoot.find("localCfg/log");
        if "path" in item.attrib:
            if len(item.attrib["path"]) > 0:
                self.m_logPath = item.attrib["path"];
        if "level" in item.attrib:
            if len(item.attrib["level"]) > 0:
                self.m_logLevel = item.attrib["level"];
        print "log cfg:" + self.m_logPath + "," + self.m_logLevel;
        #/announcementManager/localCfg/ftp
        item = xmlRoot.find("localCfg/ftp");
        if "localRootPath" in item.attrib:
            if len(item.attrib["localRootPath"]) > 0:
                self.m_localFtpRoot = item.attrib["localRootPath"];
        print "local ftp cfg:" + self.m_localFtpRoot
        #/announcementManager/autoClear
       # item = xmlRoot.find("autoClear/disk");
       # self.m_autoclearDiskPercent = item.value;
        items = xmlRoot.findall("autoClear/time");
        for i in items:
            t = i.attrib["type"];
            d = i.attrib["day"];
            if t == "xml":
                self.m_autoclearXmlDelay = d;
            elif t == "rtf":
                self.m_autoclearRtfDelay = d;
            elif t == "att":
                self.m_autoclearAttDelay = d;
        print "autoclear:" + str(self.m_autoclearDiskPercent) + "," + str(self.m_autoclearXmlDelay) + "," + str(self.m_autoclearRtfDelay) + "," + str(self.m_autoclearAttDelay);

    """
    ftp server list
    directory,key(node id),value(id:ip:port:usr:pwd)
    must config !!!
    """
    ftpCfgMap = {}
    
    """
    parse ftpConfig.xml -->ftpCfgMap
    must config!!!
    """
    def parseFtpCfg(self, fileName):
        print"parse " + fileName;
        xmlDoc = ElementTree.parse(fileName)
        xmlRoot = xmlDoc.getroot()
        nodes = xmlRoot.findall("node");
        for i in nodes:
            hostID = i.attrib["id"];
            host = i.attrib["host"];
            port = "21";
            usr = "announcement";
            pwd = "passok";
            if "port" in i.attrib:
                port = i.attrib["prot"];
            if "usr" in i.attrib:
                usr = i.attrib["usr"];
            if "pwd" in i.attrib:
                pwd = base64.b64decode(i.attrib["pwd"]);
            self.ftpCfgMap[hostID] = hostID + ":" + host + ":" + port + ":" + usr + ":" + pwd;
        print self.ftpCfgMap;
        
    """
    input:node id
    return:ftp info("id:ip:port:usr:pwd"),"::::"while not found
    """
    def getFtpCfg(self, nodeID):
        if self.ftpCfgMap.has_key(nodeID):
            return self.ftpCfgMap[nodeID];
        else:
            return "::::";

    """
    must config!!!
    """
    m_localNodeID = "";

    """
    parse local node id
    get first node id whose ip match local ip
    """
    def parseLocalNodeID(self):
        self.m_localNodeID = "";
        #local ip list
        ipList = socket.gethostbyname_ex(socket.getfqdn(socket.gethostname()));
        for nodeID,host in self.ftpCfgMap.items():
            seps = str.split(host, ":");
            if seps[1] in ipList[2]:
                self.m_localNodeID = seps[0];
                break;
 
#-------------------------------------------------
#log
def initLog():
    global g_config; 
    logPath = os.path.normpath(g_config.m_logPath);
    if not os.path.exists(logPath):
        os.makedirs(logPath);

    logger = logging.getLogger('');
    if g_config.m_logLevel.lower() == "debug" or g_config.m_logLevel.lower() == "trace":
        logger.setLevel(logging.DEBUG);
    if g_config.m_logLevel.lower() == "info":
        logger.setLevel(logging.INFO);
    if g_config.m_logLevel.lower() == "warn":
        logger.setLevel(logging.WARN);
    if g_config.m_logLevel.lower() == "error":
        logger.setLevel(logging.ERROR);
    if g_config.m_logLevel.lower() == "fatal":
        logger.setLevel(logging.FATAL);

    consoleHandler = logging.StreamHandler();
    fileHandler = logging.handlers.TimedRotatingFileHandler(os.path.normpath(logPath) + "/announcementManager.log", "D", 1, 31);
    fileHandler.suffix = "%d.log";

    fmtStr = "%(asctime)s %(levelname)-7s: %(message)s";
    datefmtStr = "%Y-%m-%d %H:%M:%S";
    formatter = logging.Formatter(fmtStr, datefmtStr);
    consoleHandler.setFormatter(formatter);    
    fileHandler.setFormatter(formatter);    

    logger.addHandler(fileHandler);
    logger.addHandler(consoleHandler);

#-------------------------------------------------
#main framework

#terminate main()
g_flagTerm = False;

#config
g_config = Config();

#signal handle
def onsignalTerm(a, b):
            logging.info("catch signal " + str(a));
            global g_flagTerm;
            g_flagTerm = True;

def main():
    global g_flagTerm;
    global g_config;

    #register signal
    signal.signal(signal.SIGTERM, onsignalTerm)
    signal.signal(signal.SIGINT, onsignalTerm)

    #parse config
    if len(sys.argv) > 1:
        g_config.parse(sys.argv[1]);
    else:
        g_config.parse();
    
    #init logging
    initLog();

    logging.info("announcement manager process start 1.0");

    if g_config.m_localNodeID != "":
        logging.info("local id: " + g_config.m_localNodeID);
        while g_flagTerm==False:
            try:
                time.sleep(5);
                checkConnectionState();
                if g_flagTerm:
                    break;                
                pushAnn();
                if g_flagTerm:
                    break;
                pushAtt();
                if g_flagTerm:
                    break;
                autoClear();
            except Exception , e:
                logging.error("main exception: " + str(e) + "," + traceback.format_exc());
    else:
        logging.error("local node id not config, exit!");

    logging.info("announcement manager process stop");
    logging.shutdown();

if __name__ == "__main__":
    main();
