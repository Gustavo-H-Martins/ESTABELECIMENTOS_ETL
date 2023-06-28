//app/routes/leads.js
/**
 * Libs
 */
const db = require('../config/db');
const logToDatabase = require('../config/log')
const router = require('express').Router();
/**
 * Conexão com o DB
 */

/**
 * Router - estrutura
 */
// GetAll
router.route("/estabelecimentos")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    const associados = req.query.associados ? req.query.associados.split(",") : null;
    const associadosTuple = associados ? `(${associados.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    const souabrasel = req.query.souabrasel ? req.query.souabrasel.split(",") : null;
    const souabraselTuple = souabrasel ? `(${souabrasel.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 100;
    const offset = (page - 1) * pageSize;
    const raio = parseFloat(req.query.raio) || 10;
    const lat = parseFloat(req.query.lat) || 0
    const lon = parseFloat(req.query.lon) || 0
    let query = `
        SELECT * FROM RECEITA
        --
        /**/
        `;
    let conditions = [];
    if (lat !== 0 && lon !== 0) conditions.push(` (ACOS(SIN(RADIANS('${lat}')) * SIN(RADIANS(LATITUDES)) + COS(RADIANS('${lat}')) * COS(RADIANS(LATITUDES)) * COS(RADIANS(LONGITUDES) - RADIANS('${lon}'))) * 6371) <= ${raio}`);
    if (bandeira) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeira) conditions.push(`BANDEIRAS LIKE "%${bandeira}%"`);
    if (associados) conditions.push(`ASSOCIADO IN ${associadosTuple}`);
    if (souabrasel) conditions.push(`SOU_ABRASEL IN ${souabraselTuple}`)
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
    if (associados == 0) query = query.replace(`ASSOCIADO IN ("0")`, `ASSOCIADO = 0`);
    if (souabrasel == 0) query = query.replace(`SOU_ABRASEL IN ("0")`, `SOU_ABRASEL = 0`);
    query += `LIMIT ? OFFSET ?;`;

    db.all(query, [pageSize, offset], (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      // Transforma a string em um array e remove os espaços em branco
        rows = rows.map(row => {
                row.BANDEIRAS = row.BANDEIRAS.split(',').map(element => element.trim());
                return row;
        });
      /*
      //console.log(query)
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
        */
      res.status(200).json(
        rows
      );
    });
  })
// GetAll-contagem
router.route("/estabelecimentos/counts")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const associados = req.query.associados ? req.query.associados.split(",") : null;
    const associadosTuple = associados ? `(${associados.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    const souabrasel = req.query.souabrasel ? req.query.souabrasel.split(",") : null;
    const souabraselTuple = souabrasel ? `(${souabrasel.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
    const groupby = req.query.groupby ? [req.query.groupby.toLocaleUpperCase()] : null;
    const orderby = req.query.orderby ? [req.query.orderby.toLocaleUpperCase()] : "DESC";
    let query = `
                  SELECT COUNT(*) AS TOTAL FROM RECEITA
                  --
                  /**/
                  ;
                  `;
    let conditions = [];
    if (bandeira) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeira) conditions.push(`BANDEIRAS LIKE "%${bandeira}%"`);
    if (associados) conditions.push(`ASSOCIADO IN ${associadosTuple}`);
    if (souabrasel) conditions.push(`SOU_ABRASEL IN ${souabraselTuple}`)
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);
    if (conditions.length > 0) {
      query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
    }
    if (groupby) query = query.replace(/\/\*\*\//g, ` GROUP BY ${groupby}`);
    if (groupby) query = query.replace(/COUNT\(\*\)/g, `${groupby}, COUNT(*)`);
    if (orderby) query = query.replace(";", `ORDER BY TOTAL ${orderby} ;`);
    if (associados == 0) query = query.replace(`ASSOCIADO IN ("0")`, `ASSOCIADO = 0`);
    if (souabrasel == 0) query = query.replace(`SOU_ABRASEL IN ("0")`, `SOU_ABRASEL = 0`);
    //console.log(query)

    db.all(query, (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      //console.log(query)
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
      res.status(200).json(
        rows
      );
    });
  })
// GET BAIRROS
router.route('/estabelecimentos/bairros')
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;

    let query = `SELECT DISTINCT(BAIRRO) FROM RECEITA
                --
                ;`;
    let conditions = [];
    if (bandeira) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeira) conditions.push(`BANDEIRAS LIKE "%${bandeira}%"`);
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);

    //console.log(query)
    db.all(query, (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      //console.log(query)
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
      res.status(200).json(
        rows
      );
    });
  })
// get cidades
router.route('/estabelecimentos/cidades')
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;

    let query = `SELECT DISTINCT(CIDADE) FROM RECEITA
                ;`;
    let conditions = [];
    if (bandeira) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeira) conditions.push(`BANDEIRAS LIKE "%${bandeira}%"`);
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);


    db.all(query, (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      //console.log(query)
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
      res.status(200).json(
        rows
      );
    });
  })

module.exports = router;