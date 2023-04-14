//app/routes/leads.js
/**
 * Libs
 */
const db = require('../config/db');
const logToDatabase = require ('../config/log')
const router = require('express').Router();
/**
 * Conexão com o DB
 */

/**
 * Router - estrutura
 */

// GetAll
router.route("/estabelecimentos")
    .get(function(req, res, next){
        const clientIp = req.ip;
        const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : `TICKET`;
        const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
        const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
        const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;
        const page = parseInt(req.query.page) || 1;
        const pageSize = parseInt(req.query.pageSize) || 100;
        const offset = (page - 1) * pageSize;

        let query = `SELECT * FROM tb_${bandeira.toLocaleLowerCase()}`;
        let conditions = [];
        if (bandeira) conditions.push(`BANDEIRA = "${bandeira}"`);
        if (uf) conditions.push(`UF = "${uf}"`);
        if (cidade) conditions.push(`CIDADE = "${cidade}"`);
        if (bairro) conditions.push(`BAIRRO = "${bairro}"`);
        if (conditions.length > 0) query += ` WHERE ${conditions.join(' AND ')}`;
        query += ` LIMIT ? OFFSET ?`;
        
        db.all(query, [pageSize, offset], (err, rows) => {
            if (err){
                res.status(500).json({error: err.message});
                return;
            }   
                // Formatando data e hora para incluir no log
                const date = new Date()
                const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
                const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
                dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
                logToDatabase(clientIp,`Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
                res.status(200).json(
                    rows
                );
        });
    });
// GetAll-contagem
router.route("/estabelecimentos/counts")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : `TICKET`;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const groupby = req.query.groupby ? [req.query.groupby.toLocaleUpperCase()] : null;

    let query = `SELECT COUNT(*) as TOTAL FROM tb_${req.query.bandeira.toLowerCase()}`;
    let conditions = [];
    if (bandeira) conditions.push(`BANDEIRA = "${bandeira}"`);
    if (uf) conditions.push(`Estado = "${uf}"`);
    if (cidade) conditions.push(`Municipio = "${cidade}"`);
    if (conditions.length > 0) query += ` WHERE ${conditions.join(' AND ')}`;
    if (groupby) query += ` GROUP BY ${groupby}`;
    if (groupby) query = query.replace('COUNT(*)', `${groupby}, COUNT(*)`)

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
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" município de ${cidade}`, 'INFO', dataChamada)
      res.status(200).json(
        rows
      );
    });
  })

module.exports = router;