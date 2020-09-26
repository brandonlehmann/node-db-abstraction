// Copyright (c) 2020 Brandon Lehmann
//
// Please see the included LICENSE file for more information.

import {EventEmitter} from 'events';
import {createTable, IDatabase, Interfaces, prepareCreateTable} from "./Types";
import {resolve} from 'path';
import {Database, OPEN_CREATE, OPEN_READWRITE} from 'sqlite3';
import {escape} from 'mysql';
import {Metronome} from "node-metronome";
import {format} from "util";

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

/** @ignore */
interface ICallback {
    callback: (error: Error | undefined, result: Interfaces.IQueryResult | undefined) => void;
}

/** @ignore */
interface ITransactionQueueEntry extends ICallback {
    queries: Interfaces.IBulkQuery[];
}

interface IQueryQueueEntry extends ICallback {
    query: string;
    values?: any[];
}

export class SQLite extends EventEmitter implements IDatabase {
    private readonly m_db: Database;
    private m_tableOptions: string | undefined = undefined;
    private m_timer: Metronome;
    private m_transactionQueue: ITransactionQueueEntry[] = [];
    private m_runQueue: IQueryQueueEntry[] = [];
    private m_allQueue: IQueryQueueEntry[] = [];

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

        this.m_timer = new Metronome(250, true);

        this.m_timer.on('tick', async () => {
            this.m_timer.paused = true;

            while (this.m_transactionQueue.length > 0) {
                const queuedTransaction = this.m_transactionQueue.shift();

                if (!queuedTransaction) {
                    break;
                }

                const connection = await this.connection();

                try {
                    await beginTransaction(connection);

                    for (const q of queuedTransaction.queries) {
                        await query(connection, q.query, q.values);
                    }

                    await commit(connection);

                    queuedTransaction.callback(undefined, undefined);
                } catch (e) {
                    await rollback(connection);

                    queuedTransaction.callback(e, undefined);
                }
            }

            while (this.m_allQueue.length > 0) {
                const queue = this.m_allQueue.shift();

                if (!queue) {
                    break;
                }

                try {
                    const result = await this.all(queue.query, queue.values);

                    queue.callback(undefined, result);
                } catch (e) {
                    queue.callback(e, undefined);
                }
            }

            while (this.m_runQueue.length > 0) {
                const queue = this.m_runQueue.shift();

                if (!queue) {
                    break;
                }

                try {
                    const result = await this.run(queue.query, queue.values);

                    queue.callback(undefined, result);
                } catch (e) {
                    queue.callback(e, undefined);
                }
            }

            this.m_timer.paused = false;
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

    public get type(): Interfaces.DBType {
        return Interfaces.DBType.SQLITE;
    }

    public on(event: 'error', listener: (error: Error) => void): this;

    public on(event: 'ready', listener: (filepath: string, mode: number) => void): this;

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

    public async getPragma(option: string): Promise<any> {
        option = option.toLowerCase();

        const query = 'PRAGMA ' + option;

        const [count, rows] = await this.query(query);

        if (count === 1) {
            return rows[0][option];
        } else {
            if (rows[0][option]) {
                return rows.map(elem => elem[option]);
            }
        }

        return rows;
    }

    public async setPragma(
        option: string,
        value: boolean | number | string
    ): Promise<void> {
        option = option.toLowerCase();

        let query = 'PRAGMA ';

        if (pragmaFunctionCalls.indexOf(option) !== -1) {
            query += option + '(' + value + ')';
        } else {
            query += option + ' = ' + value;
        }

        await this.query(query);
    }

    public async close(): Promise<void> {
        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
            const sleep = async (timeout: number = 100) =>
                new Promise(resolve => setTimeout(resolve, timeout));

            /**
             * Let's not close the database connection while we still have stuff
             * waiting to get done
             */
            while (this.m_transactionQueue.length !== 0
            || this.m_runQueue.length !== 0
            || this.m_allQueue.length !== 0) {
                await sleep();
            }

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
                this.m_allQueue.push({
                    query, values, callback:
                        (error, result) => {
                            if (error) {
                                return reject(error);
                            }

                            return resolve(result);
                        }
                })
            } else {
                this.m_runQueue.push({
                    query, values, callback:
                        (error, result) => {
                            if (error) {
                                return reject(error);
                            }

                            return resolve(result);
                        }
                })
            }
        })
    }

    public async connection(): Promise<Database> {
        return this.m_db;
    }

    public async transaction(queries: Interfaces.IBulkQuery[]): Promise<void> {
        return new Promise((resolve, reject) => {
            this.m_transactionQueue.push({
                queries: queries,
                callback: (error) => {
                    if (error) {
                        return reject(error);
                    }

                    return resolve();
                }
            })
        })
    }

    public prepareMultiInsert(table: string, columns: string[], values?: Interfaces.IValueArray): string {
        const query = format('INSERT INTO %s (%s) %L', table, columns.join(','));

        if (values) {
            const escaped = escape(values);

            return query.replace('%L', escaped);
        }

        return query;
    }

    private async all(query: string, values?: any[]): Promise<Interfaces.IQueryResult> {
        return new Promise((resolve, reject) => {
            this.m_db.all(query, values, (error, rows) => {
                if (error) {
                    return reject(error);
                }

                return resolve([rows.length, rows]);
            })
        })
    }

    private async run(query: string, values?: any[]): Promise<Interfaces.IQueryResult> {
        return new Promise((resolve, reject) => {
            this.m_db.run(query, values, function (error) {
                if (error) {
                    return reject(error);
                }

                const count = (query.toLowerCase().startsWith('insert')) ? this.lastID :
                    (query.toLowerCase().startsWith('update')
                        || query.toLowerCase().startsWith('delete')) ? this.changes : 0;

                return resolve([count, []]);
            })
        })
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
