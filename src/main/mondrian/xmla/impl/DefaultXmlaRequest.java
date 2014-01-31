/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.xmla.impl;

import mondrian.olap.Exp;
import mondrian.olap.Literal;
import mondrian.olap.Parameter;
import mondrian.olap.ParameterImpl;
import mondrian.olap.Util;
import mondrian.olap.type.BooleanType;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.xmla.*;

import org.apache.log4j.Logger;

import org.w3c.dom.*;

import java.math.BigDecimal;
import java.util.*;

import static org.olap4j.metadata.XmlaConstants.Method;

/**
 * Default implementation of {@link mondrian.xmla.XmlaRequest} by DOM API.
 *
 * @author Gang Chen
 */
public class DefaultXmlaRequest
    implements XmlaRequest, XmlaConstants
{
    private static final Logger LOGGER =
        Logger.getLogger(DefaultXmlaRequest.class);

    private static final String MSG_INVALID_XMLA = "Invalid XML/A message";

    /* common content */
    private Method method;
    private Map<String, String> properties;
    private final String roleName;

    /* EXECUTE content */
    private String statement;
    private boolean drillthrough;
    private List<Parameter> parameters;

    /* DISCOVER content */
    private String requestType;
    private Map<String, Object> restrictions;

    private final String username;
    private final String password;
    private final String sessionId;

    public DefaultXmlaRequest(
        final Element xmlaRoot,
        final String roleName,
        final String username,
        final String password,
        final String sessionId)
        throws XmlaException
    {
        init(xmlaRoot);
        this.roleName = roleName;
        this.username = username;
        this.password = password;
        this.sessionId = sessionId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Method getMethod() {
        return method;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, Object> getRestrictions() {
        if (method != Method.DISCOVER) {
            throw new IllegalStateException(
                "Only METHOD_DISCOVER has restrictions");
        }
        return restrictions;
    }

    public String getStatement() {
        if (method != Method.EXECUTE) {
            throw new IllegalStateException(
                "Only METHOD_EXECUTE has statement");
        }
        return statement;
    }

    public String getRoleName() {
        return roleName;
    }

    public String getRequestType() {
        if (method != Method.DISCOVER) {
            throw new IllegalStateException(
                "Only METHOD_DISCOVER has requestType");
        }
        return requestType;
    }

    public boolean isDrillThrough() {
        if (method != Method.EXECUTE) {
            throw new IllegalStateException(
                "Only METHOD_EXECUTE determines drillthrough");
        }
        return drillthrough;
    }

    protected final void init(Element xmlaRoot) throws XmlaException {
        if (NS_XMLA.equals(xmlaRoot.getNamespaceURI())) {
            String lname = xmlaRoot.getLocalName();
            if ("Discover".equals(lname)) {
                method = Method.DISCOVER;
                initDiscover(xmlaRoot);
            } else if ("Execute".equals(lname)) {
                method = Method.EXECUTE;
                initExecute(xmlaRoot);
            } else {
                // Note that is code will never be reached because
                // the error will be caught in
                // DefaultXmlaServlet.handleSoapBody first
                StringBuilder buf = new StringBuilder(100);
                buf.append(MSG_INVALID_XMLA);
                buf.append(": Bad method name \"");
                buf.append(lname);
                buf.append("\"");
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_BAD_METHOD_CODE,
                    HSB_BAD_METHOD_FAULT_FS,
                    Util.newError(buf.toString()));
            }
        } else {
            // Note that is code will never be reached because
            // the error will be caught in
            // DefaultXmlaServlet.handleSoapBody first
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Bad namespace url \"");
            buf.append(xmlaRoot.getNamespaceURI());
            buf.append("\"");
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_METHOD_NS_CODE,
                HSB_BAD_METHOD_NS_FAULT_FS,
                Util.newError(buf.toString()));
        }
    }

    private void initDiscover(Element discoverRoot) throws XmlaException {
        Element[] childElems =
            XmlaUtil.filterChildElements(
                discoverRoot,
                NS_XMLA,
                "RequestType");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of RequestType elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_REQUEST_TYPE_CODE,
                HSB_BAD_REQUEST_TYPE_FAULT_FS,
                Util.newError(buf.toString()));
        }
        requestType = XmlaUtil.textInElement(childElems[0]); // <RequestType>

        childElems =
            XmlaUtil.filterChildElements(
                discoverRoot,
                NS_XMLA,
                "Properties");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Properties elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_PROPERTIES_CODE,
                HSB_BAD_PROPERTIES_FAULT_FS,
                Util.newError(buf.toString()));
        }
        initProperties(childElems[0]); // <Properties><PropertyList>

        childElems =
            XmlaUtil.filterChildElements(
                discoverRoot,
                NS_XMLA,
                "Restrictions");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Restrictions elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_RESTRICTIONS_CODE,
                HSB_BAD_RESTRICTIONS_FAULT_FS,
                Util.newError(buf.toString()));
        }
        initRestrictions(childElems[0]); // <Restriciotns><RestrictionList>
    }

    private void initExecute(Element executeRoot) throws XmlaException {
        Element[] childElems =
            XmlaUtil.filterChildElements(
                executeRoot,
                NS_XMLA,
                "Command");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Command elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_COMMAND_CODE,
                HSB_BAD_COMMAND_FAULT_FS,
                Util.newError(buf.toString()));
        }
        initCommand(childElems[0]); // <Command><Statement>

        childElems =
            XmlaUtil.filterChildElements(
                executeRoot,
                NS_XMLA,
                "Properties");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Properties elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_PROPERTIES_CODE,
                HSB_BAD_PROPERTIES_FAULT_FS,
                Util.newError(buf.toString()));
        }
        initProperties(childElems[0]); // <Properties><PropertyList>

        childElems =
            XmlaUtil.filterChildElements(
                executeRoot,
                NS_XMLA,
                "Parameters");
        // <Parameters>
        if (childElems.length == 1) {
            parameters = parseParameters(childElems[0]);
        }
    }

    private void initRestrictions(Element restrictionsRoot)
        throws XmlaException
    {
        Map<String, List<String>> restrictions =
            new HashMap<String, List<String>>();
        Element[] childElems =
            XmlaUtil.filterChildElements(
                restrictionsRoot,
                NS_XMLA,
                "RestrictionList");
        if (childElems.length == 1) {
            NodeList nlst = childElems[0].getChildNodes();
            for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
                Node n = nlst.item(i);
                if (n instanceof Element) {
                    Element e = (Element) n;
                    if (NS_XMLA.equals(e.getNamespaceURI())) {
                        String key = e.getLocalName();
                        String value = XmlaUtil.textInElement(e);

                        List<String> values;
                        if (restrictions.containsKey(key)) {
                            values = restrictions.get(key);
                        } else {
                            values = new ArrayList<String>();
                            restrictions.put(key, values);
                        }

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(
                                "DefaultXmlaRequest.initRestrictions: "
                                + " key=\""
                                + key
                                + "\", value=\""
                                + value
                                + "\"");
                        }

                        values.add(value);
                    }
                }
            }
        } else if (childElems.length > 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of RestrictionList elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_RESTRICTION_LIST_CODE,
                HSB_BAD_RESTRICTION_LIST_FAULT_FS,
                Util.newError(buf.toString()));
        }

        // If there is a Catalog property,
        // we have to consider it a constraint as well.
        String key =
            org.olap4j.metadata.XmlaConstants
                .Literal.CATALOG_NAME.name();

        if (this.properties.containsKey(key)
            && !restrictions.containsKey(key))
        {
            List<String> values;
            values = new ArrayList<String>();
            restrictions.put(this.properties.get(key), values);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "DefaultXmlaRequest.initRestrictions: "
                    + " key=\""
                    + key
                    + "\", value=\""
                    + this.properties.get(key)
                    + "\"");
            }
        }

        this.restrictions = (Map) Collections.unmodifiableMap(restrictions);
    }

    private void initProperties(Element propertiesRoot) throws XmlaException {
        Map<String, String> properties = new HashMap<String, String>();
        Element[] childElems =
            XmlaUtil.filterChildElements(
                propertiesRoot,
                NS_XMLA,
                "PropertyList");
        if (childElems.length == 1) {
            NodeList nlst = childElems[0].getChildNodes();
            for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
                Node n = nlst.item(i);
                if (n instanceof Element) {
                    Element e = (Element) n;
                    if (NS_XMLA.equals(e.getNamespaceURI())) {
                        String key = e.getLocalName();
                        String value = XmlaUtil.textInElement(e);

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(
                                "DefaultXmlaRequest.initProperties: "
                                + " key=\""
                                + key
                                + "\", value=\""
                                + value
                                + "\"");
                        }

                        properties.put(key, value);
                    }
                }
            }
        } else if (childElems.length > 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of PropertyList elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_PROPERTIES_LIST_CODE,
                HSB_BAD_PROPERTIES_LIST_FAULT_FS,
                Util.newError(buf.toString()));
        } else {
        }
        this.properties = Collections.unmodifiableMap(properties);
    }


    private void initCommand(Element commandRoot) throws XmlaException {
        Element[] childElems =
            XmlaUtil.filterChildElements(
                commandRoot,
                NS_XMLA,
                "Statement");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Statement elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_STATEMENT_CODE,
                HSB_BAD_STATEMENT_FAULT_FS,
                Util.newError(buf.toString()));
        }
        statement = XmlaUtil.textInElement(childElems[0]).replaceAll("\\r", "");
        drillthrough = statement.toUpperCase().indexOf("DRILLTHROUGH") != -1;
    }

    private List<Parameter> parseParameters(Element parametersElement)
        throws XmlaException
    {
        try {
            Element[] paramElements =
                XmlaUtil.filterChildElements(
                    parametersElement,
                    NS_XMLA,
                    "Parameter");
            List<Parameter> parameters =
                new ArrayList<Parameter>(paramElements.length);
            for (Element param : paramElements) {
                String name = getChildTextValue(param, NS_XMLA, "Name");
                Element valueElement =
                    XmlaUtil.filterChildElements(param, NS_XMLA, "Value")[0];
                String value = XmlaUtil.textInElement(valueElement);
                Attr type = valueElement.getAttributeNodeNS(NS_XSI, "type");
                parameters.add(
                    new XmlaParameter(name, value, type.getValue())
                        .toParameter());
            }
            return parameters;
        } catch (Exception e) {
            throw new XmlaException(
                  CLIENT_FAULT_FC,
                  HSB_BAD_PARAMETERS_CODE,
                  HSB_BAD_PARAMETERS_FAULT_FS,
                  Util.newError(
                      e,
                      MSG_INVALID_XMLA
                      + ":" + "Error reading Parameter element"));
        }
  }

    /**
     * XMLA adomd parameter
     */
    private static class XmlaParameter {
        private static Type STRING = new StringType();
        private static Type BOOL = new BooleanType();
        private static Type NUMERIC = new NumericType();
      
        private static Map<String, Type> mondrianTypeRegistry = new HashMap<String, Type>();
        static {
            mondrianTypeRegistry.put(XmlaHandler.XSD_STRING, STRING);
            mondrianTypeRegistry.put(XmlaHandler.XSD_BOOLEAN, BOOL);

            mondrianTypeRegistry.put(XmlaHandler.XSD_DECIMAL, NUMERIC);
            mondrianTypeRegistry.put(XmlaHandler.XSD_INTEGER, NUMERIC);
            mondrianTypeRegistry.put(XmlaHandler.XSD_DOUBLE, NUMERIC);
            mondrianTypeRegistry.put(XmlaHandler.XSD_FLOAT, NUMERIC);
            mondrianTypeRegistry.put(XmlaHandler.XSD_INT, NUMERIC);
        }

        private String name, value, type;

        public XmlaParameter( String name, String value, String type ) {
            this.name = name;
            this.value = value;
            this.type = type;
        }

        Type getOlapType() {
            Type mondrianType = mondrianTypeRegistry.get(type);
            return (mondrianType != null) ? mondrianType : STRING; 
        }

        Exp getExpression() {
            Type olapType = getOlapType();
            if (olapType.equals(STRING)) {
                return Literal.createString(value);
            }
            else if (olapType.equals(NUMERIC)) {
                return Literal.create(new BigDecimal(value));
            }
            else {
                // TODO check other types, have fallback or exception
                return null;
            }
        }

        public Parameter toParameter() {
            Exp exp = getExpression();
            return new ParameterImpl(name, exp, null, exp.getType());
        }
    }

    private static String getChildTextValue(
        Element element,
        String ns,
        String childName)
    {
        Element[] children = XmlaUtil.filterChildElements(element, ns, childName);
        if (children.length > 0) {
            return XmlaUtil.textInElement(children[0]);
        }
        return null;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }
}

// End DefaultXmlaRequest.java
