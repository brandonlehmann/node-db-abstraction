// Copyright (c) 2020 Brandon Lehmann
//
// Please see the included LICENSE file for more information.

import {EventEmitter} from 'events';
import {Pool, PoolClient} from 'pg';
import * as pgformat from 'pg-format';
import {format} from 'util';
import {createTable, IDatabase, Interfaces, prepareCreateTable} from "./Types";

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
    constructor(
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
        })

        this.m_pool.on('connect', client => this.emit('connect', client));
        this.m_pool.on('acquire', client => this.emit('acquire', client));
        this.m_pool.on('remove', client => this.emit('remove', client));
        this.m_pool.on('error', (error: Error, client: PoolClient) =>
            this.emit('error', error, client));
        this.emit('ready');
    }

    public get hashType(): string {
        return 'char(64)';
    }

    public get blobType(): string {
        return 'text';
    }

    public get uint32Type(): string {
        return 'numeric(10)';
    }

    public get uint64Type(): string {
        return 'numeric(20)';
    }

    public get tableOptions(): string | undefined {
        return this.m_tableOptions;
    }

    public set tableOptions(value: string | undefined) {
        this.m_tableOptions = value;
    }

    public get type(): Interfaces.DBType {
        return Interfaces.DBType.POSTGRES;
    }

    public on(event: 'connect', listener: (client: PoolClient) => void): this;

    public on(event: 'acquire', listener: (client: PoolClient) => void): this;

    public on(event: 'remove', listener: (client: PoolClient) => void): this;

    public on(event: 'error', listener: (error: Error) => void): this;

    public on(event: 'ready', listener: () => void): this;

    public on(event: any, listener: (...args: any[]) => void): this {
        return super.on(event, listener);
    }

    public async close(): Promise<void> {
        return this.m_pool.end();
    }

    public async createTable(
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

    public prepareCreateTable(
        name: string,
        fields: Interfaces.ITableColumn[],
        primaryKey: string[],
        tableOptions?: string
    ): { table: string; indexes: string[] } {
        return prepareCreateTable(this.type, name, fields, primaryKey, tableOptions);
    }

    public async query(
        query: string,
        values?: any[],
        openClient?: PoolClient
    ): Promise<Interfaces.IQueryResult> {
        const client = openClient || await this.m_pool.connect();

        query = transformQuery(query);

        const res = await client.query(query, values);

        if (!openClient)
            await client.release();

        return [res.rowCount, res.rows];
    }

    public async transaction(queries: Interfaces.IBulkQuery[]): Promise<void> {
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

    public prepareMultiInsert(table: string, columns: string[], values?: Interfaces.IValueArray): string {
        const query = format('INSERT INTO %s (%s) %L', table, columns.join(','));

        return pgformat(query, values);
    }
}

/** @ignore */
function transformQuery(query: string): string {
    let counter = 1;

    while (query.indexOf('?') !== -1) {
        query = query.replace(/\?/, `$${counter}`);
        counter++;
    }

    return query;
}
