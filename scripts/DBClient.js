import pgFormat from 'pg-format';
import pg from 'pg';

const { Pool } = pg;

const DBClient = ({ tablePrefix = '' } = {}) => {
  const pool = new Pool({
    host: process.env.DB_HOST,
    port: 25060,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: {
      rejectUnauthorized: false
    }
  });

  const prefix = (n) => `${tablePrefix}${n}`;

  const createTable = async (name, { columns, serialId = false, uniqId = true }) => {
    const tableName = prefix(name);
    try {
      const deletionQuery = pgFormat(`DROP TABLE IF EXISTS %I`, tableName);
      await pool.query(deletionQuery);
      const tableCols = columns.map(
        (field) =>
          `${field.name} ${field.type} ${field.name === 'id' && uniqId ? 'PRIMARY KEY' : ''}`
      );
      if (serialId) {
        tableCols.unshift('id SERIAL');
        await pool.query(`ALTER SEQUENCE IF EXISTS ${tableName}_seq RESTART WITH 1`);
      }
      const creationQuery = pgFormat(`CREATE TABLE %I (%s)`, tableName, tableCols);
      await pool.query(creationQuery);
    } catch (e) {
      console.error(e);
    }
  };

  const bulkInsertData = async ({ entries, columns, table, uniq = false, returning }) => {
    let columnList = columns ? columns.map((col) => col.name) : Object.keys(entries[0]); // Depends on the first entry having all values present

    const formattedValues = entries.map((entry) =>
      columns.map((col) => {
        let val = entry[col.name];
        if (col.type.includes('[]')) val = `{${val}}`;
        if (col.type === 'int') val = val === '' || val === undefined ? null : parseInt(val);
        return val;
      })
    );

    const query = pgFormat(
      `INSERT INTO %I (%s) VALUES %L %s %s;`,
      prefix(table),
      columnList,
      formattedValues,
      uniq ? 'ON CONFLICT (id) DO NOTHING' : '',
      returning ? `RETURNING ${returning}` : ''
    );

    const result = await pool.query(query);
    return result.rows.map((row) => row.id);
  };

  const simpleSelect = async ({ table, asObjectOn, columns = ['*'], rest = '' }) => {
    const query = pgFormat('SELECT %s from %I %s', columns, prefix(table), rest);
    const result = await pool.query(query);
    let data = result.rows;

    if (asObjectOn) {
      data = result.rows.reduce((acc, row) => {
        acc[row[asObjectOn]] = row;
        return acc;
      }, {});
    }
    return { data };
  };

  const createIndex = async ({ table, on }) => {
    //create index new_idx_objective_transaction_transaction_id on new_objective_transaction(transaction_id);
    const name = `${tablePrefix}idx_${table}_${on}`;
    const dropQuery = pgFormat(`DROP INDEX IF EXISTS %s`, name);
    const createQuery = pgFormat(`CREATE INDEX %s ON %I(%I)`, name, prefix(table), on);
    await pool.query(dropQuery);
    await pool.query(createQuery);
  };

  const count = async ({ table }) => {
    const query = pgFormat('SELECT count(*) from %I', prefix(table));
    const result = await pool.query(query);
    return parseInt(result.rows[0].count);
  };

  const ciao = () => pool.end();

  return {
    bulkInsertData,
    createTable,
    simpleSelect,
    count,
    createIndex,
    ciao
  };
};

export default DBClient;
