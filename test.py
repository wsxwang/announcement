#!/usr/bin/python
#-*-coding:utf-8-*-
"""
announcementManager.py test
"""

import announcementManager as am

def setup():
    print "setup";

def teardown():
    print "teardown";

def test_isAnnName():
    assert am.isAnnName("20140101_100000_0000");
    assert not am.isAnnName("20140101_0000");
    assert not am.isAnnName("20140101_100000_a000");
    assert not am.isAnnName("20140101_a000");
    assert am.isAnnName("20140101_100000_0000_000");
    assert am.isAnnName("20140101_112000_0000_0000");
    assert am.isAnnName("20140101_123456_0000_00000");
    assert not am.isAnnName("20140101");
    assert not am.isAnnName("20140101_0");
    assert not am.isAnnName("20140101_");
    assert not am.isAnnName("2014010");

def test_chooseNodes4Push():
    nodes = ["100000:ip:21:u:p","120000:ip:21:u:p","130000:ip:21:u:p","120001:ip:21:u:p","120002:ip:21:u:p", "130001:ip:21:u:p", "131002:ip:21:u:p"];
    
    am.g_config.m_localNodeID = "100000";
    pNodes = am.chooseNodes4Push("30000101_0000", nodes);
    assert "100000:ip:21:u:p" not in pNodes;
    assert "120000:ip:21:u:p" in pNodes;
    assert "130000:ip:21:u:p" in pNodes;
    assert "120001:ip:21:u:p" not in pNodes;
    assert "120002:ip:21:u:p" not in pNodes;
    assert "130001:ip:21:u:p" not in pNodes;
    assert "131002:ip:21:u:p" not in pNodes;
    
    am.g_config.m_localNodeID = "120000";
    pNodes = am.chooseNodes4Push("20000101_0000", nodes);
    assert "100000:ip:21:u:p" in pNodes;
    assert "120000:ip:21:u:p" not in pNodes;
    assert "130000:ip:21:u:p" not in pNodes;
    assert "120001:ip:21:u:p" in pNodes;
    assert "120002:ip:21:u:p" in pNodes;
    assert "130001:ip:21:u:p" not in pNodes;
    assert "131002:ip:21:u:p" not in pNodes;
    
    am.g_config.m_localNodeID = "120001";
    pNodes = am.chooseNodes4Push("20000101_0000", nodes);
    assert "100000:ip:21:u:p" not in pNodes;
    assert "120000:ip:21:u:p" in pNodes;
    assert "130000:ip:21:u:p" not in pNodes;
    assert "120001:ip:21:u:p" not in pNodes;
    assert "120002:ip:21:u:p" not in pNodes;
    assert "130001:ip:21:u:p" not in pNodes;
    assert "131002:ip:21:u:p" not in pNodes;

def test_nodeidtype():
    assert am.isRootID("100000");
    assert not am.isRootID("110000");
    assert am.isProvinceID("110000");
    assert not am.isProvinceID("111000");
    assert am.isProvinceID("320000");
    assert am.isCityID("320100");
    assert am.isCityID("321000");
    assert am.getUpNodeID("100000") == "";
    assert am.getUpNodeID("110000") == "100000";
    assert am.getUpNodeID("320000") == "100000";
    assert am.getUpNodeID("320100") == "320000";
    assert am.getUpNodeID("321000") == "320000";
    assert am.getUpNodeID("1") == "";
   
if __name__ == "__main__":
    setup();

    test_isAnnName();
    test_chooseNodes4Push();
    test_nodeidtype();
    
    teardown();
