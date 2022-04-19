<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>jstream-root</artifactId>
        <groupId>io.github.shanqiang-sq</groupId>
        <version>1.0.29</version>
        <relativePath>../pom.xml</relativePath>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>jstream</artifactId>
    <packaging>jar</packaging>

    <properties>
        <odps.version>0.38.3-public</odps.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>io.github.shanqiang-sq</groupId>
            <artifactId>jstream-shaded-jar</artifactId>
        </dependency>

        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>8.0.26</version>
        </dependency>

        <dependency>
            <groupId>commons-codec</groupId>
            <artifactId>commons-codec</artifactId>
            <version>1.15</version>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>2.8.0</version>
        </dependency>

        <dependency>
            <groupId>net.jpountz.lz4</groupId>
            <artifactId>lz4</artifactId>
            <version>1.3.0</version>
        </dependency>

        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-all</artifactId>
            <version>4.1.67.Final</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.odps</groupId>
            <artifactId>odps-sdk-core</artifactId>
            <version>${odps.version}</version>
            <exclusions>
                <exclusion>
                    <artifactId>protobuf-java</artifactId>
                    <groupId>com.google.protobuf</groupId>
                </exclusion>
            </exclusions>
        </dependency>

        <dependency>
            <groupId>com.aliyun.odps</groupId>
            <artifactId>odps-sdk-commons</artifactId>
            <version>${odps.version}</version>
        </dependency>

        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
            <version>29.0-jre</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.7.30</version>
        </dependency>

        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.2.6</version>
        </dependency>

        <dependency>
            <groupId>org.openjdk.jol</groupId>
            <artifactId>jol-core</artifactId>
            <version>0.2</version>
        </dependency>

        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>units</artifactId>
            <version>1.6</version>
        </dependency>

        <dependency>
            <groupId>com.github.oshi</groupId>
            <artifactId>oshi-core</artifactId>
            <version>5.2.0</version>
        </dependency>

        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
            <version>3.14.0</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.openservices</groupId>
            <artifactId>aliyun-log</artifactId>
            <version>0.6.64</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.openservices</groupId>
            <artifactId>loghub-client-lib</artifactId>
            <version>0.6.41</version>
        </dependency>

        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>