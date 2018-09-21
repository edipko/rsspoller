package com.spotonresponse.bigdata.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.json.XML;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GetRssFeed {

    private static final Logger logger = LogManager.getLogger(GetRssFeed.class);
    private static ClassLoader classloader = Thread.currentThread().getContextClassLoader();


    public GetRssFeed() {

    }


    public static JSONObject getFeed(URL url) {

        String xmlString = "";

        try {
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            // Setup a new eventReader
            logger.debug("Opening input stream: " + url.toString());

            InputStream in = url.openStream();
            XMLStreamReader streamReader = inputFactory.createXMLStreamReader(in);

            logger.debug("Opening stylesheet");
            InputStream phonestyle_is = classloader.getResourceAsStream("stylesheet.xslt");
            StreamSource stylesource = new StreamSource(phonestyle_is);


            logger.debug("Applying Stylesheet");
            TransformerFactory tfactory = TransformerFactory.newInstance();
            //Transformer transformer = tfactory.newTransformer();
            Transformer transformer = tfactory.newTransformer(stylesource);
            StringWriter stringWriter = new StringWriter();
            transformer.transform(new StAXSource(streamReader), new StreamResult(stringWriter));


            xmlString = stringWriter.toString();
            logger.debug("Got XML as string");

        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }

        return XML.toJSONObject(xmlString);

    }

    public static JSONObject getFeed(String path) {
        String newXmlString = "";

        try {
            Charset encoding = Charset.defaultCharset();
            byte[] encoded = Files.readAllBytes(Paths.get(path));
            String xmlString = new String(encoded, encoding);


            // Remove attributes and namespaces
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            InputStream xmlStream = new ByteArrayInputStream(xmlString.getBytes());
            Document doc = builder.parse(xmlStream);
            xmlStream.close();

            InputStream phonestyle_is = classloader.getResourceAsStream("stylesheet.xslt");
            StreamSource stylesource = new StreamSource(phonestyle_is);

            TransformerFactory tfactory = TransformerFactory.newInstance();
            Transformer transformer = tfactory.newTransformer(stylesource);

            StreamResult result = new StreamResult(new StringWriter());
            DOMSource source = new DOMSource(doc);
            transformer.transform(source, result);
            xmlString = result.getWriter().toString();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        }
        return XML.toJSONObject(newXmlString);
    }

    public static String getFeedString(URL url) {

        String xmlString = "";

        try {
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            // Setup a new eventReader
            logger.debug("Opening input stream: " + url.toString());

            InputStream in = url.openStream();
            XMLStreamReader streamReader = inputFactory.createXMLStreamReader(in);

            logger.debug("Opening stylesheet");
            InputStream phonestyle_is = classloader.getResourceAsStream("stylesheet.xslt");
            StreamSource stylesource = new StreamSource(phonestyle_is);


            logger.debug("Not applying Stylesheet");
            TransformerFactory tfactory = TransformerFactory.newInstance();
            Transformer transformer = tfactory.newTransformer();
            StringWriter stringWriter = new StringWriter();
            transformer.transform(new StAXSource(streamReader), new StreamResult(stringWriter));


            xmlString = stringWriter.toString();
            logger.debug("Got XML as string");

        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }

        return xmlString;

    }
}
