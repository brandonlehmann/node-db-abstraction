// Copyright (c) 2020 Brandon Lehmann
//
// Please see the included LICENSE file for more information.

import { format } from 'util';

export namespace Interfaces {
    /**
     * The database type
     */
    export enum DBType {
        MYSQL,
        POSTGRES,
        SQLITE
    }

    /**
     * Bulk Query interface that allows us to specify a single query with
     * a multidimensional array of values
     */
    export interface IBulkQuery {
        query: string;
        values?: any[];
    }

    /**
     * Foreign Key interface that represents a foreign key relationship
     * with a defined constraint
     */
    export interface IForeignKey {
        table: string;
        column: string;
        update?: string;
        delete?: string;
    }

    /**
     * Column interface that represents an individual table column
     */
    export interface ITableColumn {
        name: string;
        type: string;
        nullable?: boolean;
        foreign?: IForeignKey;
        unique?: boolean;
        default?: string | number
    }

    /**
     * Foreign key constraint action enum
     */
    export enum FKAction {
        RESTRICT = 'RESTRICT',
        CASCADE = 'CASCADE',
        NULL = 'SET NULL',
        DEFAULT = 'SET DEFAULT',
        NA = 'NO ACTION'
    }

    /**
     * Represents a query result such that [0] is the number of rows returned,
     * affected, or changed, depending on the query verb
     */
    export type IQueryResult = [number, any[]];

    /**
     * A nested array of values
     */
    export type IValueArray = any[][];
}

/**
 * Abstract database class that all underlying database
 * classes must implement so that we have consistency in methods
 */
export abstract class IDatabase {
    /**
     * Returns the column type for the DB for a hash (ie. char(64))
     */
    public abstract get hashType(): string;

    /**
     * Returns the column type for the DB to store a hexadecimal blob of data
     */
    public abstract get blobType(): string;

    /**
     * Returns the column type for the DB to store a uint32_t value
     */
    public abstract get uint32Type(): string;

    /**
     * Returns the column type for the DB to store a uint64_t value
     */
    public abstract get uint64Type(): string;

    /**
     * Returns a string of additional options that need to be applied to tables
     * which may include ENGINE information, compression, etc depending on the DB system
     */
    public abstract get tableOptions(): string | undefined;

    /**
     * Returns the database type
     */
    public abstract get type(): Interfaces.DBType;

    /**
     * Creates the table with the given fields and options
     * @param name
     * @param fields
     * @param primaryKey
     * @param tableOptions
     */
    public abstract async createTable(
        name: string,
        fields: Interfaces.ITableColumn[],
        primaryKey: string[],
        tableOptions?: string
    ): Promise<void>;

    /**
     * Creates the SQL statements necessary to create a table with the given
     * fields and options
     * @param name
     * @param fields
     * @param primaryKey
     * @param tableOptions
     */
    public abstract prepareCreateTable(
        name: string,
        fields: Interfaces.ITableColumn[],
        primaryKey: string[],
        tableOptions?: string
    ): { table: string, indexes: string[] };

    /**
     * Closes the database connection(s)
     */
    public abstract async close(): Promise<void>;

    /**
     * Performs a query and returns the result
     * @param query
     * @param values
     */
    public abstract async query(query: string, values?: any[]): Promise<Interfaces.IQueryResult>;

    /**
     * Constructs and executes the given queries in a transaction
     * @param queries
     */
    public abstract async transaction(queries: Interfaces.IBulkQuery[]): Promise<void>;

    /**
     * Prepares a query to perform a multi-insert statement which is
     * far faster than a whole bunch of individual insert statements
     * @param table
     * @param columns
     * @param values
     */
    public abstract prepareMultiInsert(table: string, columns: string[], values?: Interfaces.IValueArray): string;

    /**
     * Prepares a query to perform a multi-update statement via UPSERT
     * style statements which is far faster than a whole bunch of
     * individual update statements in most of the DBMS platforms
     * @param query
     * @param values
     */
    // public abstract prepareMultiUpdate(query: string, values?: Interfaces.IValueArray): string;
}

/**
 * Prepares a CREATE TABLE statement for the underlying database
 * type and builds the primary keys and fields based on the supplied
 * values
 * @param dbType
 * @param name
 * @param fields
 * @param primaryKey
 * @param tableOptions
 */
export function prepareCreateTable (
    dbType: Interfaces.DBType,
    name: string,
    fields: Interfaces.ITableColumn[],
    primaryKey: string[],
    tableOptions: string | undefined
): { table: string, indexes: string[] } {
    const l_fields = fields.map(col =>
        format(
            '%s %s %s %s',
            col.name,
            col.type,
            (!col.nullable) ? 'NOT NULL' : 'NULL',
            (typeof col.default !== 'undefined') ? 'DEFAULT ' + col.default : ''
        ));

    const l_unique = fields.filter(elem => elem.unique === true)
        .map(col => format(
            'CREATE UNIQUE INDEX IF NOT EXISTS %s_unique_%s ON %s (%s);',
            name,
            col.name,
            name,
            col.name
        ));

    const constraint_fmt =
        ', CONSTRAINT %s_%s_fkey FOREIGN KEY (%s) REFERENCES %s (%s)';

    const l_constraints: string[] = [];

    for (const field of fields) {
        if (field.foreign) {
            let constraint = format(
                constraint_fmt,
                name,
                field.name,
                field.name,
                field.foreign.table,
                field.foreign.column
            );

            if (field.foreign.delete) {
                constraint += format(' ON DELETE %s', field.foreign.delete.toUpperCase());
            }

            if (field.foreign.update) {
                constraint += format(' ON UPDATE %s', field.foreign.update.toUpperCase());
            }

            l_constraints.push(constraint);
        }
    }

    const sql = format(
        'CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s)%s) %s;',
        name,
        l_fields.join(', '),
        primaryKey.join(','),
        l_constraints.join(','),
        tableOptions || ''
    ).trim();

    return {
        table: sql,
        indexes: l_unique
    };
}

/**
 * Executes the result of the prepare create table and returns if completed
 * @param db
 * @param dbType
 * @param name
 * @param fields
 * @param primaryKey
 * @param tableOptions
 */
export async function createTable (
    db: IDatabase,
    dbType: Interfaces.DBType,
    name: string,
    fields: Interfaces.ITableColumn[],
    primaryKey: string[],
    tableOptions?: string
): Promise<void> {
    const preparedTable = prepareCreateTable(dbType, name, fields, primaryKey, tableOptions);

    await db.transaction([{ query: preparedTable.table }]);

    if (preparedTable.indexes.length !== 0) {
        const stmts: Interfaces.IBulkQuery[] = preparedTable.indexes.map(query => {
            return { query };
        });

        await db.transaction(stmts);
    }
}
