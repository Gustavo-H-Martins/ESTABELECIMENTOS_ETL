/*coletor_leads_voucher/app/routes/geolocalizacao*/
// função para buscar latitude com base no endereço ou relativo
// Importar o módulo node-geocoder
const NodeGeocoder = require('node-geocoder');

// Configurar as opções do geocodificador
const options = {
  provider: 'google', // ou outro provedor de sua escolha
  apiKey: 'YOUR_API_KEY', // somente necessário se usar um provedor que exija uma chave
};

// Criar um objeto geocodificador
const geocoder = NodeGeocoder(options);

// Obter a latitude e longitude a partir do nome da cidade ou bairro e cidade
// Definir a função diretamente no módulo.exports
module.exports = async function busca_localizacao(local) {
  try {
    const res = await geocoder.geocode(local); // aguarda a promessa ser resolvida
    let lat = res[0].latitude; // obtém a latitude
    let lon = res[0].longitude; // obtém a longitude
    console.log(lat); // exibe a latitude
    console.log(lon); // exibe a longitude
    return [lat, lon]; // retorna um array com os valores de latitude e longitude
  } catch (err) {
    console.error(err); // trata o erro se ocorrer
  }
}

// Atribuir a função a uma variável e depois exportá-la
var busca_localizacao = async function(local) {
  try {
    const res = await geocoder.geocode(local); // aguarda a promessa ser resolvida
    let lat = res[0].latitude; // obtém a latitude
    let lon = res[0].longitude; // obtém a longitude
    console.log(lat); // exibe a latitude
    console.log(lon); // exibe a longitude
    return [lat, lon]; // retorna um array com os valores de latitude e longitude
  } catch (err) {
    console.error(err); // trata o erro se ocorrer
    // podemos usar err.message ou err.code para obter mais detalhes sobre o erro
    // podemos usar if/else ou switch/case para tratar diferentes tipos de erro
    // podemos retornar um valor padrão ou lançar uma exceção dependendo da sua lógica de negócio
  }
}

module.exports = busca_localizacao;
