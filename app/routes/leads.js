//app/routes/leads.js
/**
 * Libs
 */
const db = require('../config/db');
const logToDatabase = require('../config/log')
const router = require('express').Router();
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

// definindo a secretkey só a título de teste
// verifica o jwt
function verificaJWT(req, res, next) {
  const token = req.headers['authorization'];
  const SECRETKEY = req.headers['x-api-key'];
  
  // Verifique o token JWT para extrair o userId
  jwt.verify(token, SECRETKEY, (err, decoded) => {
      if (err) return res.status(401).end();
      
      // Consulte o banco de dados para obter a secretKey do usuário
      db.get('SELECT secretKey FROM users WHERE userId = ?', [decoded.userId], (err, row) => {
          if (err || !row) {
              // Ocorreu um erro ao consultar o banco de dados ou o usuário não foi encontrado
              res.status(401).end();
          } else {
              // Verifique novamente o token JWT usando a secretKey do usuário
              jwt.verify(token, row.secretKey, (err, decoded) => {
                  if (err) return res.status(401).end();
                  
                  req.userId = decoded.userId;
                  next();
              });
          }
      });
  });
}
/**
 * Conexão com o DB
 */

/**
 * Router - estrutura
 */
// registro

router.route("/register")
.post(function(req, res, next) {
    const username = req.body.username;
    const password = req.body.password;
    //const secretKey = crypto.randomBytes(64).toString('hex');
    const buffer = crypto.randomBytes(64);
    //console.log(buffer); // 64

    const secretKey = buffer.toString('hex');
    //console.log(secretKey); // 128
    db.get('SELECT * FROM tb_usuario WHERE username = ? AND password = ?', [username, password], (err, row) => {
      if (err) {
        res.status(500).send('Ocorreu um erro ao verificar as credenciais do usuário');
      } else if (!row) {
    // Insira as informações do usuário na tabela de usuários do banco de dados
    db.run('INSERT INTO tb_usuario (username, password, secretKey) VALUES (?, ?, ?)', [username, password, secretKey], (err) => {
        if (err) {
            // Ocorreu um erro ao inserir os dados
            res.status(500).send('Ocorreu um erro ao registrar o usuário');
        } else {
            // Os dados foram inseridos com sucesso
            res.send('Registro bem-sucedido');
        }
    })
    } else if (row.username === username) {
      res.status(401).send('usuario já cadastrado tente fazer logi!')
    }
});
});
// Login
router.route("/login")
.post(function (req, res) {
  const { username, password } = req.body;
  
  // Consulte o banco de dados para verificar se há um registro na tabela de usuários com o mesmo nome de usuário e senha
  db.get('SELECT * FROM tb_usuario WHERE username = ? AND password = ?', [username, password], (err, row) => {
      if (err) {
          // Ocorreu um erro ao consultar o banco de dados
          res.status(500).send('Ocorreu um erro ao verificar as credenciais do usuário');
      } else if (row) {
          // As credenciais do usuário são válidas
          const SECRETKEY = row.secretKey
          const token = jwt.sign({ userId: row.userId },SECRETKEY , { expiresIn: 3000 });
          res.json({
              auth: true,
              token,
              SECRETKEY,
              expiresIn: 3000
          });
      } else {
          // As credenciais do usuário são inválidas
          res.status(401).end();
      }
  });
});
// GetAll
router.route("/estabelecimentos")
  
  .get(verificaJWT,function(req, res, next) {
    console.log(`O ${req.userId} fez esta chamada!`)
    const clientIp = req.ip;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 100;
    const offset = (page - 1) * pageSize;

    let query = `
                  SELECT * FROM tb_ticket
                  --
                  /**/
                  `;
    let conditions = [];
    if (bandeira) query = `SELECT * FROM tb_${[req.query.bandeira.toLowerCase()]}`;
    if (bandeira) conditions.push(`BANDEIRA = "${bandeira}"`);
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
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

// GetAllRFB
router.route("/estabelecimentos_rfb")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const associados = req.query.associados ? [req.query.associados.toUpperCase()] : null;
    const souabrasel = req.query.souabrasel ? [req.query.souabrasel.toUpperCase()] : null;
    const tembandeira = req.query.tembandeira ? [req.query.tembandeira.toUpperCase()] : "TICKET";
    const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 100;
    const offset = (page - 1) * pageSize;
    const raio = parseFloat(req.query.raio) || 10;
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
                    rfb.CEP AS CEP,
                    s.ENDERECO AS ENDERECO_SIGA,
                    t.ENDERECO AS ENDERECO_TICKET,
                    a.ENDERECO AS ENDERECO_ALELO,
                    rfb.BAIRRO AS BAIRRO,
                    rfb.CIDADE AS CIDADE,
                    rfb.UF AS UF,
                    t.TELEFONE AS TELEFONE_TICKET,
                    a.TELEFONE AS TELEFONE_ALELO,
                    rfb.TELEFONE AS TELEFONE_RFB,
                    rfb.EMAIL,
                        CASE 
                            WHEN t.CNPJ IS NULL THEN False 
                            ELSE True
                        END AS TEM_TICKET,
                        CASE 
                            WHEN a.RAZAO_SOCIAL IS NULL THEN False 
                            ELSE True
                        END AS TEM_ALELO,
                        CASE 
                            WHEN s.CNPJ IS NULL THEN False 
                            ELSE True 
                        END AS BASE_SIGA,
                        CASE 
                            WHEN s.ASSOCIADO IS NULL THEN False
                            WHEN s.ASSOCIADO = 'ATIVO' THEN 'ATIVO'
                            WHEN s.ASSOCIADO = 'INATIVO' THEN 'INATIVO'
                        END AS ASSOCIADO,
                        CASE
                            WHEN s.SOU_ABRASEL IS NULL THEN False
                            WHEN s.SOU_ABRASEL = 'ATIVO' THEN 'ATIVO'
                            WHEN s.SOU_ABRASEL = 'INATIVO' THEN 'INATIVO'
                            WHEN s.SOU_ABRASEL = 'DESCOMISSIONADO' THEN 'DESCOMISSIONADO'
                        END AS SOU_ABRASEL,
                        m.LATITUDE,
                        m.LONGITUDE
                FROM tb_rfb rfb
                LEFT JOIN tb_ticket t ON t.CNPJ = rfb.CNPJ
                LEFT JOIN tb_alelo a ON a.RAZAO_SOCIAL = rfb.RAZAO_SOCIAL
                LEFT JOIN tb_municipios m ON m.BAIRRO = rfb.BAIRRO AND m.UF = rfb.UF
                LEFT JOIN tb_siga s ON s.CNPJ = rfb.CNPJ
                --
                /**/
                  `;
    let conditions = [];
    if (lat !== 0 && lon !== 0) conditions.push(` (ACOS(SIN(RADIANS('${lat}')) * SIN(RADIANS(LATITUDES)) + COS(RADIANS('${lat}')) * COS(RADIANS(LATITUDES)) * COS(RADIANS(LONGITUDES) - RADIANS('${lon}'))) * 6371) <= ${raio}`);
    if (bandeira) conditions.push(`${bandeira[0].toLowerCase()}.bandeira = ${bandeira}`);
    if (tembandeira) conditions.push(`TEM_${tembandeira} = 1`);
    if (associados) conditions.push(`ASSOCIADO = "${associados}"`);
    if (souabrasel) conditions.push(`SOU_ABRASEL = "${souabrasel}"`)
    if (uf) conditions.push(`rfb.UF = "${uf}"`);
    if (cidade) conditions.push(`rfb.CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`rfb.BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
    query += `LIMIT ? OFFSET ?;`;

    db.all(query, [pageSize, offset], (err, rows) => {
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
// GetAll-contagem
router.route("/estabelecimentos/counts")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;
    const groupby = req.query.groupby ? [req.query.groupby.toLocaleUpperCase()] : null;
    const orderby = req.query.orderby ? [req.query.orderby.toLocaleUpperCase()] : null;
    let query = `
                  SELECT COUNT(*) AS TOTAL FROM tb_ticket

                  --
                  /**/
                  ;
                  `;
    let conditions = [];
    if (bandeira) query = `SELECT COUNT(*) AS TOTAL FROM tb_${[req.query.bandeira.toLowerCase()]}`
    if (bandeira) conditions.push(`BANDEIRA = "${bandeira}"`);
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
    if (groupby) query = query.replace(/\/\*\*\//g, ` GROUP BY ${groupby}`);
    if (groupby) query = query.replace(/COUNT\(\*\)/g, `${groupby}, COUNT(*)`);
    if (orderby) query = query.replace(";", `ORDER BY TOTAL ${orderby} ;`);
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
// GetAll-contagem
router.route("/estabelecimentos_rfb/counts")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const associados = req.query.associados ? [req.query.associados.toUpperCase()] : null;
    const souabrasel = req.query.souabrasel ? [req.query.souabrasel.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;
    const groupby = req.query.groupby ? [req.query.groupby.toLocaleUpperCase()] : null;
    const orderby = req.query.orderby ? [req.query.orderby.toLocaleUpperCase()] : null;
    let query = `
                /*TICKET COM CNPJ_RFB E SIGA*/
                SELECT 
                    CASE WHEN rfb.CNPJ IS NULL THEN False 
                    ELSE True END AS 'CNAE_AFL',
                    COUNT(*) AS TOTAL
                FROM tb_ticket t
                LEFT JOIN tb_rfb rfb  ON t.CNPJ = rfb.CNPJ
                LEFT JOIN tb_siga s ON s.CNPJ  = rfb.CNPJ
                --
                /**/
                ;
                `;
    let conditions = [];
    if (bandeira) conditions.push(`t.BANDEIRA = "${bandeira}"`);
    if (associados) conditions.push(`s.ASSOCIADO = "${associados}"`);
    if (souabrasel) conditions.push(`s.SOU_ABRASEL = "${souabrasel}"`)
    if (uf) conditions.push(`t.UF = "${uf}"`);
    if (cidade) conditions.push(`t.CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`t.BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
    if (groupby) query = query.replace(/\/\*\*\//g, ` GROUP BY t.${groupby}`);
    if (groupby) query = query.replace(/COUNT\(\*\)/g, `t.${groupby}, COUNT(*)`);
    if (orderby) query = query.replace(";", `ORDER BY TOTAL ${orderby} ;`);
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
router.route('/estabelecimentos/bairros')
  .get(function(req, res, next) {
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
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
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
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const bandeira = req.query.bandeira ? [req.query.bandeira.toUpperCase()] : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairro = req.query.bairro ? [req.query.bairro.toUpperCase().replace(/-/g, ' ')] : null;

    let query = `SELECT DISTINCT(CIDADE) FROM tb_ticket
                ;`;
    let conditions = [];
    if (bandeira) conditions.push(`BANDEIRA = "${bandeira}"`);
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairro) conditions.push(`BAIRRO = "${bairro}"`);
    if (conditions.length > 0) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
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