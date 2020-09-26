// Copyright (c) 2020 Brandon Lehmann
//
// Please see the included LICENSE file for more information.

import {EventEmitter} from 'events';
import {createTable, IDatabase, Interfaces, prepareCreateTable} from "./Types";
import {createPool, escape, Pool, PoolConnection} from 'mysql';

/**
 * MYSQL interface that implements IDatabase
 */
export class MySQL extends EventEmitter implements IDatabase {
    private readonly m_db: Pool;
    private m_tableOptions: string | undefined = 'ENGINE=InnoDB PACK_KEYS=1 ROW_FORMAT=COMPRESSED';

    /**
     * Creates a new instance of the class
     * @param host
     * @param port
     * @param user
     * @param password
     * @param database
     * @param connectionLimit
     */
    constructor(
        host: string,
        port: number,
        user: string,
        password: string,
        database: string,
        connectionLimit: number = 10
    ) {
        super();

        this.m_db = createPool({host, port, user, password, database, connectionLimit})
        this.m_db.on('error', error => this.emit('error', error));
        this.m_db.on('acquire', connection => this.emit('acquire', connection));
        this.m_db.on('connection', connection => this.emit('connection', connection));
        this.m_db.on('enqueue', () => this.emit('enqueue'));
        this.m_db.on('release', connection => this.emit('release', connection));
        this.emit('ready');
    }

    public get hashType(): string {
        return 'char(64)';
    }

    public get blobType(): string {
        return 'longtext';
    }

    public get uint32Type(): string {
        return 'int(10) unsigned';
    }

    public get uint64Type(): string {
        return 'bigint(20) unsigned';
    }

    public get tableOptions(): string | undefined {
        return this.m_tableOptions;
    }

    public set tableOptions(value: string | undefined) {
        this.m_tableOptions = value;
    }

    public get type(): Interfaces.DBType {
        return Interfaces.DBType.MYSQL;
    }

    public on(event: 'error', listener: (error: Error) => void): this;

    public on(event: 'acquire', listener: (connection: PoolConnection) => void): this;

    public on(event: 'connection', listener: (connection: PoolConnection) => void): this;

    public on(event: 'enqueue', listener: () => void): this;

    public on(event: 'release', listener: (connection: PoolConnection) => void): this;

    public on(event: 'ready', listener: () => void): this;

    public on(event: any, listener: (...args: any[]) => void): this {
        return super.on(event, listener);
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

    public async close(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.m_db.end(err => {
                if (err) return reject(err);

                return resolve();
            })
        })
    }

    public async query(
        query: string,
        values?: any[]
    ): Promise<Interfaces.IQueryResult> {
        return new Promise((resolve, reject) => {
            this.m_db.query(query, values || [], (error, results) => {
                if (error) return reject(error);

                const count = results.changedRows ||
                    results.affectedRows ||
                    results.insertId ||
                    results.length;

                return resolve([count, results])
            })
        })
    }

    public async transaction(queries: Interfaces.IBulkQuery[]): Promise<void> {
        const connection = await this.connection();

        try {
            await beginTransaction(connection);

            for (const q of queries) {
                await query(connection, q.query, q.values);
            }

            await commit(connection);
        } catch (e) {
            await rollback(connection);
            throw e;
        } finally {
            connection.release();
        }
    }

    public prepareMultiInsert(query: string, values?: Interfaces.IValueArray): string {
        if (values) {
            const escaped = escape(values);

            return query.replace('%L', escaped);
        }

        return query;
    }

    private async connection(): Promise<PoolConnection> {
        return new Promise((resolve, reject) => {
            this.m_db.getConnection((error, connection) => {
                if (error) return reject(error);

                return resolve(connection);
            })
        })
    }
}

/** @ignore */
async function beginTransaction(connection: PoolConnection): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.beginTransaction(error => {
            if (error) return reject(error);

            return resolve();
        })
    })
}

/** @ignore */
async function commit(connection: PoolConnection): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.commit(error => {
            if (error) return reject(error);

            return resolve();
        })
    })
}

/** @ignore */
async function query(
    connection: PoolConnection,
    query: string,
    values: any[] = []
): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.query(query, values, (error) => {
            if (error) return reject(error);

            return resolve();
        })
    })
}

/** @ignore */
async function rollback(connection: PoolConnection): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.rollback(error => {
            if (error) return reject(error);

            return resolve();
        })
    })
}
