// Copyright (c) 2020 Brandon Lehmann
//
// Please see the included LICENSE file for more information.

import { EventEmitter } from 'events';
import { Pool, PoolClient } from 'pg';
import * as pgformat from 'pg-format';
import { format } from 'util';
import { createTable, IDatabase, Interfaces, prepareCreateTable } from './Types';

/**
 * Postgres database abstraction interface
 */
export class Postgres extends EventEmitter implements IDatabase {
    private readonly m_pool: Pool;
    private m_tableOptions: string | undefined = undefined;

    /**
     * Creates a new instance of the class
     * @param host
     * @param port
     * @param username
     * @param password
     * @param database
     */
    constructor (
        host: string,
        port: number,
        username: string,
        password: string,
        database: string
    ) {
        super();

        this.m_pool = new Pool({
            user: username,
            password: password,
            host: host,
            port: port,
            database: database,
            ssl: {
                rejectUnauthorized: false
            }
        });

        this.m_pool.on('connect', client => this.emit('connect', client));
        this.m_pool.on('acquire', client => this.emit('acquire', client));
        this.m_pool.on('remove', client => this.emit('remove', client));
        this.m_pool.on('error', (error: Error, client: PoolClient) =>
            this.emit('error', error, client));
        this.emit('ready');
    }

    /**
     * Returns the column type for the DB for a hash (ie. char(64))
     */
    public get hashType (): string {
        return 'char(64)';
    }

    /**
     * Returns the column type for the DB to store a hexadecimal blob of data
     */
    public get blobType (): string {
        return 'text';
    }

    /**
     * Returns the column type for the DB to store a uint32_t value
     */
    public get uint32Type (): string {
        return 'numeric(10)';
    }

    /**
     * Returns the column type for the DB to store a uint64_t value
     */
    public get uint64Type (): string {
        return 'numeric(20)';
    }

    /**
     * Returns a string of additional options that need to be applied to tables
     * which may include ENGINE information, compression, etc depending on the DB system
     */
    public get tableOptions (): string | undefined {
        return this.m_tableOptions;
    }

    public set tableOptions (value: string | undefined) {
        this.m_tableOptions = value;
    }

    /**
     * Returns the database type
     */
    public get type (): Interfaces.DBType {
        return Interfaces.DBType.POSTGRES;
    }

    public on(event: 'connect', listener: (client: PoolClient) => void): this;

    public on(event: 'acquire', listener: (client: PoolClient) => void): this;

    public on(event: 'remove', listener: (client: PoolClient) => void): this;

    public on(event: 'error', listener: (error: Error) => void): this;

    public on(event: 'ready', listener: () => void): this;

    public on (event: any, listener: (...args: any[]) => void): this {
        return super.on(event, listener);
    }

    /**
     * Closes the database connection(s)
     */
    public async close (): Promise<void> {
        return this.m_pool.end();
    }

    /**
     * Creates the table with the given fields and options
     * @param name
     * @param fields
     * @param primaryKey
     * @param tableOptions
     */
    public async createTable (
        name: string,
        fields: Interfaces.ITableColumn[],
        primaryKey: string[],
        tableOptions?: string
    ): Promise<void> {
        try {
            await createTable(this, this.type, name, fields, primaryKey, tableOptions);
        } catch (error) {
            this.emit('error', error);
        }
    }

    /**
     * Creates the SQL statements necessary to create a table with the given
     * fields and options
     * @param name
     * @param fields
     * @param primaryKey
     * @param tableOptions
     */
    public prepareCreateTable (
        name: string,
        fields: Interfaces.ITableColumn[],
        primaryKey: string[],
        tableOptions?: string
    ): { table: string; indexes: string[] } {
        return prepareCreateTable(this.type, name, fields, primaryKey, tableOptions);
    }

    /**
     * Performs a query and returns the result
     * @param query
     * @param values
     * @param openClient permit specifying the pool client to use
     */
    public async query (
        query: string,
        values?: any[],
        openClient?: PoolClient
    ): Promise<Interfaces.IQueryResult> {
        const client = openClient || await this.m_pool.connect();

        query = transformQuery(query);

        const res = await client.query(query, values);

        if (!openClient) { await client.release(); }

        return [res.rowCount, res.rows];
    }

    /**
     * Constructs and executes the given queries in a transaction
     * @param queries
     */
    public async transaction (queries: Interfaces.IBulkQuery[]): Promise<void> {
        const client = await this.m_pool.connect();

        try {
            await client.query('BEGIN');

            for (const q of queries) {
                await this.query(q.query, q.values, client);
            }

            await client.query('COMMIT');
        } catch (e) {
            await client.query('ROLLBACK');
            throw e;
        } finally {
            await client.release();
        }
    }

    /**
     * Prepares a query to perform a multi-insert statement which is
     * far faster than a whole bunch of individual insert statements
     * @param table the table to perform the insert on
     * @param columns the columns to include in the insert
     * @param values the value of the columns
     */
    public prepareMultiInsert (table: string, columns: string[], values?: Interfaces.IValueArray): string {
        const query = format('INSERT INTO %s (%s) %L', table, columns.join(','));

        return pgformat(query, values);
    }

    /**
     * Prepares a query to perform a multi-update statement which is
     * based upon a multi-insert statement that performs an UPSERT
     * and this is a lot faster than a whole bunch of individual
     * update statements
     * @param table the table to perform the insert on
     * @param primaryKey the primary key(s) of the column for update purposes
     * @param columns the columns to update in the table
     * @param values the value of the columns
     */
    public prepareMultiUpdate (
        table: string, primaryKey: string[], columns: string[], values?: Interfaces.IValueArray): string {
        const query = this.prepareMultiInsert(table, primaryKey.concat(columns), values);

        const updates: string[] = [];

        for (const column of columns) {
            updates.push(format(' %s = excluded.%s', column, column));
        }

        return format('%s ON CONFLICT (%s) DO UPDATE SET %s', query, primaryKey.join(','), updates.join(','));
    }
}

/** @ignore */
function transformQuery (query: string): string {
    let counter = 1;

    while (query.indexOf('?') !== -1) {
        query = query.replace(/\?/, `$${counter}`);
        counter++;
    }

    return query;
}
