<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
    <xsl:output method="xml" indent="yes" omit-xml-declaration="yes"/>
    <xsl:template match="@* | node()[not(self::*)]">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
    <xsl:template match="rss/@version"/>
    <!--<xsl:template match="rss" />
    -->
    <xsl:template match="*/phone">
        <xsl:element name="{local-name()}">
            <xsl:copy-of select="@*"/>
            <xsl:value-of select="concat(substring(., 1, 3), '-', substring(., 4, 3), '-', substring(., 7, 4))"/>
        </xsl:element>

    </xsl:template>


    <xsl:template match="*">
        <xsl:element name="{local-name()}">
            <xsl:apply-templates select="@* | node()"/>
        </xsl:element>
    </xsl:template>

</xsl:stylesheet>