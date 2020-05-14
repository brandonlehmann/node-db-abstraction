// Copyright (c) 2020 Brandon Lehmann
//
// Please see the included LICENSE file for more information.

import {format} from "util";

export namespace Interfaces {
    /**
     * Bulk Query interface that allows us to specify a single query with
     * a multidimensional array of values
     */
    export interface IBulkQuery {
        query: string;
        values?: any[];
    }

    /**
     * Column interface that represents an individual table column
     */
    export interface ITableColumn {
        name: string;
        type: string;
        nullable?: boolean;
        foreign?: IForeignKey;
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
     * Prepares a CREATE TABLE statement for mass execution
     * @param name
     * @param fields
     * @param primaryKey
     * @param tableOptions
     */
    public static prepareCreateTable(
        name: string,
        fields: Interfaces.ITableColumn[],
        primaryKey: string[],
        tableOptions: string | undefined
    ): string {
        const l_fields = fields.map(col =>
            format(
                '%s %s %s',
                col.name,
                col.type,
                (!col.nullable) ? 'NOT NULL' : 'NULL'
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

                l_constraints.push(constraint)
            }
        }

        return format(
            'CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s)%s) %s',
            name,
            l_fields.join(', '),
            primaryKey.join(','),
            l_constraints.join(','),
            tableOptions || ''
        ).trim();
    }

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
     * @param query
     * @param values
     */
    public abstract prepareMultiInsert(query: string, values?: Interfaces.IValueArray): string;
}
