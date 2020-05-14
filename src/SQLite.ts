// Copyright (c) 2020 Brandon Lehmann
//
// Please see the included LICENSE file for more information.

import {EventEmitter} from 'events';
import {Interfaces, IDatabase} from "./Types";
import {resolve} from 'path';
import {Database, OPEN_CREATE, OPEN_READWRITE} from 'sqlite3';
import {escape} from 'mysql';

/** @ignore */
const pragmaFunctionCalls = [
    'quick_check',
    'integrity_check',
    'incremental_vacuum',
    'foreign_key_check',
    'foreign_key_list',
    'index_info',
    'index_list',
    'index_xinfo',
    'table_info',
    'table_xinfo',
    'optimize'
]

export class SQLite extends EventEmitter implements IDatabase {
    private readonly m_db: Database;
    private m_tableOptions: string | undefined = undefined;

    /**
     * Creates a new instance of the class
     * @param filepath
     * @param mode
     */
    constructor(
        filepath: string,
        mode: number = OPEN_READWRITE | OPEN_CREATE
    ) {
        super();

        filepath = resolve(filepath);

        this.m_db = new Database(filepath, error => {
            if (error) return this.emit('error', error);

            this.emit('ready', filepath, mode);
        })

        this.on('ready', async () => {
            try {
                await this.setPragma('foreign_keys', true);
            } catch (e) {
                this.emit('error', e);
            }
        })
    }

    public get hashType(): string {
        return 'varchar(64)';
    }

    public get blobType(): string {
        return 'text';
    }

    public get uint32Type(): string {
        return 'unsigned int';
    }

    public get uint64Type(): string {
        return 'unsigned bigint';
    }

    public get tableOptions(): string | undefined {
        return this.m_tableOptions;
    }

    public set tableOptions(value: string | undefined) {
        this.m_tableOptions = value;
    }

    public on(event: 'error', listener: (error: Error) => void): this;

    public on(event: 'ready', listener: (filepath: string, mode: number) => void): this;

    public on(event: any, listener: (...args: any[]) => void): this {
        return super.on(event, listener);
    }

    public async getPragma(option: string): Promise<any> {
        return new Promise((resolve, reject) => {
            option = option.toLowerCase();

            let query = 'PRAGMA ' + option;

            this.m_db.all(query, (error, rows) => {
                if (error) return reject(error);

                if (rows.length === 1) {
                    return resolve(rows[0][option]);
                } else {
                    if (rows[0][option]) {
                        return resolve(rows.map(elem => elem[option]));
                    }
                }

                return resolve(rows);
            })
        })
    }

    public async setPragma(
        option: string,
        value: boolean | number | string
    ): Promise<void> {
        return new Promise((resolve, reject) => {
            option = option.toLowerCase();

            let query = 'PRAGMA ';

            if (pragmaFunctionCalls.indexOf(option) !== -1) {
                query += option + '(' + value + ')';
            } else {
                query += option + ' = ' + value;
            }

            this.m_db.run(query, error => {
                if (error) return reject(error);

                return resolve();
            })
        })
    }

    public async close(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.m_db.close(error => {
                if (error) return reject(error);

                return resolve();
            })
        })
    }

    public async query(
        query: string,
        values?: any[]
    ): Promise<Interfaces.IQueryResult> {
        return new Promise((resolve, reject) => {
            if (query.toLowerCase().startsWith('select')) {
                this.m_db.all(query, values, (err, rows) => {
                    if (err) return reject(err);

                    return resolve([rows.length, rows]);
                })
            } else {
                this.m_db.run(query, values, function (err) {
                    if (err) return reject(err);

                    const count = (query.toLowerCase().startsWith('insert')) ? this.lastID :
                        (query.toLowerCase().startsWith('update')
                            || query.toLowerCase().startsWith('delete')) ? this.changes : 0;

                    return resolve([count, []]);
                })
            }
        })
    }

    public async connection(): Promise<Database> {
        return this.m_db;
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
        }
    }

    public prepareMultiInsert(query: string, values?: Interfaces.IValueArray): string {
        if (values) {
            const escaped = escape(values);

            return query.replace('%L', escaped);
        }

        return query;
    }
}

/** @ignore */
async function beginTransaction(connection: Database): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.run('BEGIN TRANSACTION', err => {
            if (err) return reject(err);

            return resolve();
        })
    })
}

/** @ignore */
async function commit(connection: Database): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.run('COMMIT', err => {
            if (err) return reject(err);

            return resolve();
        })
    })
}

/** @ignore */
async function query(
    connection: Database,
    query: string,
    values: any[] = []
): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.run(query, values, err => {
            if (err) return reject(err);

            return resolve();
        })
    })
}

/** @ignore */
async function rollback(connection: Database): Promise<void> {
    return new Promise((resolve, reject) => {
        connection.run('ROLLBACK', err => {
            if (err) return reject(err);

            return resolve();
        })
    })
}
