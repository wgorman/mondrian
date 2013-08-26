/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.*;

/**
 * Variant of {@link MySqlDialect} that uses ANSI join syntax. For testing.
 */
public class MySqlAnsiJoinDialect extends MySqlDialect {
    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            MySqlAnsiJoinDialect.class,
            DatabaseProduct.MYSQL)
        {
            protected boolean acceptsConnection(Connection connection) {
                try {
                    // Infobright looks a lot like MySQL. If this is an
                    // Infobright connection, yield to the Infobright dialect.
                    return super.acceptsConnection(connection)
                        && !isInfobright(connection.getMetaData());
                } catch (SQLException e) {
                    throw Util.newError(
                        e, "Error while instantiating dialect");
                }
            }
        };

    public MySqlAnsiJoinDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public boolean allowsJoinOn() {
        return true;
    }
}

// End MySqlAnsiJoinDialect.java
