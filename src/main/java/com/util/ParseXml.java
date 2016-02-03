package com.util;
import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ParseXml{
	public static Document getDoc(String path) throws ParserConfigurationException, SAXException, IOException {
		File fXmlFile = new File(path);
    	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    	Document doc = dBuilder.parse(fXmlFile);
    	doc.getDocumentElement().normalize();
    	return doc;
	}
	public static NodeList getNodeList(String path,String name) throws ParserConfigurationException, SAXException, IOException {
		Document doc = getDoc(path);
		NodeList nList = doc.getElementsByTagName(name);
		return nList;
	}
}