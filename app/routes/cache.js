const db = require('../config/db');

// Função para salvar os dados em cache no banco de dados SQLite
function salvaCache(query, data) {
    const tableName = 'tb_cache'; // Nome da tabela de cache
  
    // Verifique se a tabela de cache existe. Se não existir, crie-a.
    db.serialize(() => {
      db.run(`CREATE TEMPORARY TABLE IF NOT EXISTS ${tableName} (query TEXT PRIMARY KEY, data TEXT)`, (err) => {
        if (err) {
          console.error('Erro ao criar tabela de cache:', err.message);
          return;
        }
  
        // Insira ou atualize os dados de cache na tabela
        const insertQuery = `INSERT OR REPLACE INTO ${tableName} (query, data) VALUES (?, ?)`;
        db.run(insertQuery, [query, JSON.stringify(data)], (err) => {
          if (err) {
            console.error('Erro ao salvar cache no banco de dados:', err.message);
            return;
          }
  
          console.log('Cache salvo no banco de dados:', query);
        });
      });
    });
  }
  
// Função para ler os dados em cache do banco de dados SQLite
function lerCache(query) {
    const tableName = 'tb_cache'; // Nome da tabela de cache
  
    return new Promise((resolve, reject) => {
      // Verifique se a tabela de cache existe
      db.get(`SELECT name FROM sqlite_master WHERE type='table' AND name='${tableName}'`, (err, row) => {
        if (err) {
          console.error('Erro ao verificar a existência da tabela de cache:', err.message);
          reject(err);
          return;
        }
  
        if (!row) {
          console.log('Tabela de cache não existe.');
          resolve(null);
          return;
        }
  
        // Execute a consulta para obter os dados do cache
        const selectQuery = `SELECT data FROM ${tableName} WHERE query = ?`;
        db.get(selectQuery, [query], (err, row) => {
          if (err) {
            console.error('Erro ao ler cache do banco de dados:', err.message);
            reject(err);
            return;
          }
  
          if (!row) {
            console.log('Cache não encontrado no banco de dados.');
            resolve(null);
            return;
          }
  
          const data = JSON.parse(row.data);
          console.log('Cache lido do banco de dados:', query);
          resolve(data);
        });
      });
    });
  }  

module.exports = { salvaCache, lerCache };
