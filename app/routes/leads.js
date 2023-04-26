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
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 100;
    const offset = (page - 1) * pageSize;
    const raio =  parseFloat(req.query.raio) || 10;
    const lat = parseFloat(req.query.lat) || 0
    const lon = parseFloat(req.query.lon) || 0
    let query = `
                /*CNPJ_RFB COM TICKET*/
                SELECT 
                    rfb.CNPJ AS CNPJ,
                    rfb.RAZAO_SOCIAL AS RAZAO_SOCIAL_RFB,
                    rfb.NOME_FANTASIA AS NOME_FANTASIA_RFB,
                    s.NOME_FANTASIA AS NOME_FANTASIA_SIGA,
                    t.ESTABELECIMENTOS as ESTABELECIMENTOS_TICKET,
                    rfb.ENDERECO AS ENDERECO_RFB,
                    s.ENDERECO AS ENDERECO_SIGA,
                    rfb.BAIRRO AS BAIRRO,
                    rfb.CIDADE AS CIDADE,
                    rfb.UF AS UF,
                    t.TELEFONE AS TELEFONE_TICKET,
                    substr(replace(rfb.TELEFONE, '+', ' '), 4, 2) || ' ' || substr(replace(rfb.TELEFONE, '+', ' '), 6, 4) || '-' || substr(replace(rfb.TELEFONE, '+', ' '), 10) AS TELEFONE_RFB,
                    rfb.EMAIL,
                        CASE 
                            WHEN t.CNPJ IS NULL THEN False 
                            ELSE True
                        END AS TEM_TICKET,
                        CASE 
                            WHEN s.CNPJ IS NULL THEN False 
                            ELSE True 
                        END AS BASE_SIGA,
                        CASE 
                            WHEN s.ASSOCIADO IS NULL THEN False
                            WHEN s.ASSOCIADO IS 'ATIVO' THEN 'ATIVO'
                            WHEN s.ASSOCIADO IS 'INATIVO' THEN 'INATIVO'
                        END AS ASSOCIADO,
                        CASE
                            WHEN s.SOU_ABRASEL IS NULL THEN False
                            WHEN s.SOU_ABRASEL IS 'ATIVO' THEN 'ATIVO'
                            WHEN s.SOU_ABRASEL IS 'INATIVO' THEN 'INATIVO'
                            WHEN s.SOU_ABRASEL IS 'DESCOMISSIONADO' THEN 'DESCOMISSIONADO'
                        END AS SOU_ABRASEL,
                        m.LATITUDE AS LATITUDE,
                        m.LONGITUDE AS LONGITUDE
                FROM tb_rfb rfb
                LEFT JOIN tb_ticket t ON t.CNPJ = rfb.CNPJ
                LEFT JOIN tb_siga s ON s.CNPJ = rfb.CNPJ
                LEFT JOIN tb_municipios m ON m.BAIRRO = rfb.BAIRRO AND m.CIDADE = rfb.CIDADE AND m.UF = rfb.UF
                --
                /**/
                  `;
    let conditions = [];
    if(lat !== 0 && lon !== 0) conditions.push(` (ACOS(SIN(RADIANS('${lat}')) * SIN(RADIANS(m.LATITUDE)) + COS(RADIANS('${lat}')) * COS(RADIANS(m.LATITUDE)) * COS(RADIANS(m.LONGITUDE) - RADIANS('${lon}'))) * 6371) <= ${raio}`)
    if (uf) conditions.push(`rfb.UF = "${uf}"`);
    if (cidade) conditions.push(`rfb.CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`rfb.BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g,` WHERE ${conditions.join(' AND ')}`);
    query += ` LIMIT ? OFFSET ?;`;

    db.all(query, [pageSize, offset], (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
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
// GetAll-contagem
router.route("/estabelecimentos/counts")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] :null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;
    const groupby = req.query.groupby ? [req.query.groupby.toLocaleUpperCase()] : null;

    let query = `
                  /*TICKET COM CNPJ_RFB*/
                  SELECT 
                      CASE WHEN rfb.CNPJ IS NULL THEN False 
                      ELSE True END AS 'CNAE_AFL',
                      COUNT(*) AS TOTAL
                  FROM tb_ticket t
                  LEFT JOIN tb_rfb rfb ON rfb.CNPJ = t.CNPJ
                  --
                  /**/
                  ;
                  `;
    let conditions = [];
    if (bandeira) conditions.push(`t.BANDEIRA = "${bandeira}"`);
    if (uf) conditions.push(`rfb.UF = "${uf}"`);
    if (cidade) conditions.push(`rfb.CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`rfb.BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g,` WHERE ${conditions.join(' AND ')}`);
    if (groupby) query = query.replace(/\/\*\*\//g, ` GROUP BY t.${groupby}`);
    if (groupby) query = query.replace(/COUNT\(\*\)/g, `t.${groupby}, COUNT(*)`)
    console.log(query)

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
router.route('/estabelecimentos/bairros')
  .get(function (req, res, next) {
    const clientIp = req.ip;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;

    let query = `SELECT DISTINCT(BAIRRO) FROM tb_ticket
                --
                ;`;
    let conditions = [];
    if (bandeira) conditions.push(`BANDEIRA = "${bandeira}"`);
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g,` WHERE ${conditions.join(' AND ')}`);
    if (bandeira) query = `SELECT DISTINCT(BAIRRO) FROM tb_${[req.query.bandeira.toLowerCase()]}`

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

  router.route('/estabelecimentos/cidades')
  .get(function (req, res, next) {
    const clientIp = req.ip;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;

    let query = `SELECT DISTINCT(CIDADE) FROM tb_ticket
                --
                ;`;
    let conditions = [];
    if (bandeira) conditions.push(`BANDEIRA = "${bandeira}"`);
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g,` WHERE ${conditions.join(' AND ')}`);
    if (bandeira) query = `SELECT DISTINCT(CIDADE) FROM tb_${[req.query.bandeira.toLowerCase()]}`

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