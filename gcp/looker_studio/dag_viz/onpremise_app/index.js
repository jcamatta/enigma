const cytoscape = require('cytoscape'); // Libreria para crear grafos
const visNetwork = require("vis-network/peer/esm/vis-network"); // Libreria para visualizar grafos
const visData = require("vis-data/peer/esm/vis-data"); // Libreria para pasarle data a visNetwork
const options = require('./config.js'); // Options para el visNetwork
const dscc = require('@google/dscc'); // Libreria de Google para conectarse con LookerStudio

// Retorna un date en UTC
function parseDateTime(value) {
    const year = parseInt(value.substring(0, 4), 10);
    const month = parseInt(value.substring(4, 6), 10) - 1;
    const day = parseInt(value.substring(6, 8), 10);
    const hours = parseInt(value.substring(8, 10), 10);
    const minutes = parseInt(value.substring(10, 12), 10);
    const seconds = parseInt(value.substring(12), 10);
    const datetime = new Date(year, month, day, hours, minutes, seconds);
    return datetime;
}

// Parse value segun el data type
function parseData(headerType, value) {
    if (value === null || value === "") {
        return NULL_REPLACE;
    } else {
        if (headerType === 'NUMBER') {
            return Number(value);
    
        } else if (headerType === 'YEAR_MONTH_DAY_SECOND') {
            return parseDateTime(value);
        } else {
            return String(value);
        }
    }
}

// Lee la data que le llega desde LookerStudio (container es un param en desuso)
function readData(data, container) {
    const headers = data.tables.DEFAULT.headers;
    const cy = cytoscape();

    data.tables.DEFAULT.rows.forEach((row) => {
        const [node, dependency, ...attributes] = row;
        
        // Objeto para almacenar el nodo en cy.
        const nodeData = {id: node};
        attributes.forEach( (value, index) => {
            let header = headers[index + 2];
            nodeData[header.name] = parseData(header.type, value);
        })

        // Si existe actualizamos los atributos.
        let existingNode = cy.getElementById(node);
        if (existingNode.length > 0) {
            existingNode.data(nodeData);
        } else {
            cy.add({data: nodeData});
        }

        // Objeto similar a nodeData para almacenar el edge.
        const edgeData = {id: `${dependency}->${node}`, source: dependency, target: node};
        const existingDependency = cy.getElementById(dependency);
        // Si dependency es nulo, entonces no se almacena el edge.
        if (dependency !== null) {
            // Si dependency es un nodo todavia no almacenado en cy, lo almacenamos.
            if (!existingDependency.length > 0) {
                cy.add({data: {id: dependency}});
            }
            cy.add({data: edgeData});
            }
        })
    return cy;
}

// Devuelve un objeto donde cada key es un header de data con un numero de valores unicos <= maxAllowed
function getDistinctAttributeValues(cy, data, maxAllowed) {
    const attributes = data.tables.DEFAULT.headers.map( (value) => value.name).slice(2);
    const output = {};
    if (attributes.length === 0) {
        return null;
    }

    attributes.forEach( (attribute) => {
        const nodesWithAttribute = cy.nodes(`[${attribute}]`);
        const attributeValues = nodesWithAttribute.reduce((values, node) => {
            const value = node.data(attribute);

            if (!values.includes(value) && value !== null) {
                values.push(value);
            }

            return values;
        }, [])

        if (attributeValues.length > 0 && attributeValues.length <= maxAllowed ) {
            output[attribute] = attributeValues;
        }
    })
    return output;
}

// Crea un opcion (html element) con el valor dado y lo almacena en el container (select).
function createOption(value, container) {
    let option = document.createElement('option');
    option.value = value;
    option.text = value;
    container.appendChild(option);
}

// Crea un select html element y lo almacena en container.
function createSelect(label, options, container) {

    let selectContainer = document.createElement('div');
    selectContainer.id = `${label}-div`;

    let select = document.createElement('select');
    select.name = label;
    select.multiple = true;

    options.forEach((option) => {
        createOption(option, select);
    })

    createOption(IGNORE, select);

    const labelElement = document.createElement('label');
    labelElement.innerHTML = `${label.toUpperCase()}`

    selectContainer.appendChild(labelElement);
    selectContainer.appendChild(select);
    container.appendChild(selectContainer);
}

// Obtiene los valores de las selecciones que se hicieron para filtrar los nodos a mostrar.
function getFilters(container) {
    let output = {};
    let selectElements = container.getElementsByTagName('select');
    for (let i = 0; i < selectElements.length; i++) {
        let select = selectElements[i];
        let selectedValue = Array.from(select.selectedOptions).map(option => option.value);
        output[select.name] = selectedValue;
    }
    return output;
}

// Filtra los nodos en cy por aquellos que cumplan los atributos en filterAttributes.
function filterNodes(cy, filterAttributes) {
    let output = cy.nodes().filter( (node) => {
        for (let attributeName in filterAttributes) {
            let nodeValue = node.data(attributeName);
            let filterValues = filterAttributes[attributeName];
            if (filterValues.length === 0  || filterValues.includes(IGNORE)) {
                continue;
                
            } else {
                if (!filterValues.includes(String(nodeValue))) {
                    return false;
                }
            }            
        }
        return true
    })
    return output.map((element) => element.id());
}

// Retorna una lista de nodos, edges predecesores de initialNode. 
function findAllNodes(cy, initialNode) {
    let startPoint = cy.getElementById(initialNode);
    let nodesAndEdges = startPoint.predecessors();
    let edges = [];
    let nodes = [{id: startPoint.id(), label: startPoint.id(), ...startPoint.data()}];
    nodesAndEdges.forEach((element) => {
        if (element.isEdge()) {
            edges.push({from: element.source().id(), to: element.target().id()})
        } else {
            nodes.push({id: element.id(), label: element.id(), ...element.data()});
        }
    })
    return {nodes: nodes, edges: edges};
}

// Esconde todos los nodos que no contengan la palabra inputValue.
// Es para facilitar la busqueda de los nodos en el input box.
function updateOptions(inputValue, selectElement) {
    let k = null;
    for (let i = 0; i < selectElement.options.length; i++) {
        const option = selectElement.options[i];
        const optionValue = option.value;
        const shouldShow = optionValue.toLowerCase().includes(inputValue.toLowerCase());
        option.style.display = shouldShow ? 'block' : 'none';

        if (shouldShow) {
            k = i;
        }
    }
    selectElement.value = k !== null ? selectElement.options[k].value : k;
}

function hasProperties(node, propertiesList) {
    for (let i = 0; i < propertiesList.length; i++) {
        let value = propertiesList[i];
        if (!node.data().hasOwnProperty(value)) {
            return false;
        }
    }
    return true;
}

// Funcion especifica para aquellas fuentes de datos con una columna fecha
// Suma a la fecha los minutos dados.
function addMinuteToDate(datetime, minuteInt) {
    let outputDatetime = new Date(datetime);
    let hours = Math.floor(minuteInt / 60);
    let restMinute = minuteInt % 60;
    outputDatetime.setHours(outputDatetime.getHours() + hours, outputDatetime.getMinutes() + restMinute, 0, 0);
    return outputDatetime
}

// Estima la fecha para el nodo dado a partir de sumar la duracion_promedio del proceso a su fecha_inicio.
function guessDate(cy, nodeID) {
    let node = cy.getElementById(nodeID);

    // Para ejecutar esta funcion es necesario que se cuente con las siguientes propiedades.
    if (!hasProperties(node, ['duracion_promedio', 'fecha_inicio', 'fecha_fin', 'estado'])) {
        return null;
    }

    let duracion_promedio = node.data('duracion_promedio');

    if (node.data('estado') === 1) {
        return node.data('fecha_fin');

    } else if (node.data('estado') === 0) {
        node.data('fecha_fin', addMinuteToDate(node.data('fecha_inicio'), duracion_promedio));
        node.data('estimado', ['fecha_fin']);
        return node.data('fecha_fin');
    }

   
    incomers = node.incomers().nodes().map(value => value.id());

    if (incomers.includes('ROOT') || incomers.includes('DIARIO')) {
        return null;
    }

    fecha_inicio = parseDateTime('00000000000000');
    incomers.forEach((incomer) => {
        let fecha_fin = guessDate(cy, incomer);
        console.log(fecha_fin, fecha_inicio);
        if (fecha_fin > fecha_inicio) {
            fecha_inicio = fecha_fin;
        }
    })

    node.data('estimado', ['fecha_inicio', 'fecha_fin']);
    node.data('fecha_inicio', fecha_inicio);
    node.data('fecha_fin', addMinuteToDate(fecha_inicio, duracion_promedio));
    return node.data('fecha_fin');
}

function mapEstado(estadoValue) {

    if (estadoValue === null || estadoValue === NULL_REPLACE) {
        return 'WAITING';
    } else {
        estadoValue = Number(estadoValue);
        if (estadoValue === 0) {
            return 'RUNNING';
        } else if (estadoValue === -1) {
            return 'ERROR';
        } else {
            return 'SUCCESS';
        }
    }
}

// Funcion que convierte el datetime a string con el timezone ARG
function dateToTimezone(datetime) {
    if (datetime === null) {
        return null;
    }
    return datetime.toLocaleString('en-US', {timezone: TIMEZONE});
}

// Asigna propiedades a los nodos como el: color, title. Que se visualizan en la red.
function assignProperties(nodes) {
    nodes.forEach((node) => {
        if (node.hasOwnProperty('estado')) {
            node['estado'] = mapEstado(node['estado']);
            node['color'] = ESTADO_COLOR[node['estado']].color;
            node['font'] = ESTADO_COLOR[node['estado']].font;
        }

        if (node.hasOwnProperty('fecha_inicio') && node.hasOwnProperty('fecha_fin')) {
            node['fecha_inicio'] = dateToTimezone(node['fecha_inicio']);
            node['fecha_fin'] = dateToTimezone(node['fecha_fin']);

            if (node.hasOwnProperty('estimado')) {
                node['title'] = `Estimacion de [${node['estimado']}]
                            fecha_inicio = ${node['fecha_inicio']}
                            fecha_fin = ${node['fecha_fin']}
                            duracion_promedio = ${node['duracion_promedio']}
                            estado = ${node['estado']}`
            } else {
                node['title'] = `fecha_inicio = ${node['fecha_inicio']}
                            fecha_fin = ${node['fecha_fin']}
                            duracion_promedio = ${node['duracion_promedio']}
                            estado = ${node['estado']}`
            }  
        }
        return node
    })
    return nodes;
    
}

// Crea una visualizacion del grafo a partir de los nodes, edges pasados.
function createNetwork(nodes, edges, options, container) {
    // Calcula el espaciado entre nodos en el árbol de dependencias
    let maxLabelLength = Math.max(...nodes.map(node => node.label.length));
    let nodeSpacing = (nodes.length > 0) ? (maxLabelLength * 15) : 200;
    options.layout.hierarchical.nodeSpacing = nodeSpacing;

    let visNodes = new visData.DataSet(nodes);
    let visEdges = new visData.DataSet(edges);
    let networkData = { nodes: visNodes, edges: visEdges };
    let network = new visNetwork.Network(container, networkData, options);
    return network;
}

// CONSTANTES
const TIMEZONE = 'America/Argentina/Buenos_Aires';
const IGNORE = 'IGNORE';
const NULL_REPLACE = 'NO_DATA';
const ESTADO_COLOR = {
    RUNNING: {
        color: 'rgba(70, 150, 50, 0.8)',
        font: {
            color: 'white',
            size: 20,
            face: 'monospace'
        }
    },ERROR: {
        color: 'rgba(255, 0, 0, 0.8)',
        font: {
            color: 'white',
            size: 20,
            face: 'monospace'
        }
    },WAITING: {
        color: 'rgba(0, 0, 0, 0.8)',
        font: {
            color: 'white',
            size: 20,
            face: 'monospace'
        }
    },SUCCESS: {
        color: 'rgba(0, 150, 255, 0.8)',
        font: {
            color: 'grey',
            size: 20,
            face: 'monospace'
        }
    }
};

const ESTIMADO = '20px monospace grey';

// HTML ELEMENT CONSTANTES

// PRINCIPAL DIV PARA INPUT
const inputDiv = document.createElement('div');
inputDiv.id = 'input-div';

// Aca se almacenan los distintos select segun los headers/atributos de la data entrante
const attributeDiv = document.createElement('div');
attributeDiv.id = 'attribute-div';

// Boton asociado al attributeDiv para filtrar
const attributeButton = document.createElement('button');
attributeButton.id = 'attribute-button';
attributeButton.innerHTML = '<span>FILTRAR NODOS</span>';

// input element que sirve como buscador 
const inputNode = document.createElement('input');
inputNode.id = 'node-input';
inputNode.type = 'text';
inputNode.placeholder = 'Escribe el modulo a observar ...';

// selectNodes es un select con nodos como opciones.
const selectNodes = document.createElement('select');
selectNodes.id = 'node-select';

// Boton asociado al selectNode, cuando se hace click genera el grafo asociado.
const nodeButton = document.createElement('button');
nodeButton.id = 'node-button';
nodeButton.innerHTML = '<span>MOSTRAR RED</span>';

// DIV en el que se almacenan los botones.
const buttonDiv = document.createElement('div');
buttonDiv.id = 'button-div';

// PRINCIPAL DIV PARA ALMACENAR EL GRAFO
const networkDiv = document.createElement('div');
networkDiv.id = 'network-div';

buttonDiv.appendChild(attributeButton);
buttonDiv.appendChild(nodeButton);

inputDiv.appendChild(attributeDiv);
inputDiv.appendChild(inputNode);
inputDiv.appendChild(selectNodes);
inputDiv.appendChild(buttonDiv);


document.body.appendChild(inputDiv);
document.body.appendChild(networkDiv);


// Punto de entrada y funcion principal que llama a las otras.
function drawViz(data) {

    // Primero obtenemos todos los html element necesarios.
    const inputDiv = document.getElementById('input-div');
    const attributeDiv = document.getElementById('attribute-div');
    const attributeButton = document.getElementById('attribute-button');
    const inputNode = document.getElementById('node-input');
    const selectNodes = document.getElementById('node-select');
    const nodeButton = document.getElementById('node-button');
    const networkDiv = document.getElementById('network-div');
    
    // Creamos el cy object
    const cy = readData(data, networkDiv);

    // Obtenemos el objeto con key como header/atributo y cada valor una lista de valores unicos.
    const attributes = getDistinctAttributeValues(cy, data, 10);

    // Creamos cada uno de los select object para cada key en attributes
    attributeDiv.innerHTML = '';
    Object.keys(attributes).forEach( (value) => {createSelect(value, attributes[value], attributeDiv)});
    inputDiv.style.setProperty('--num-of-childs', inputDiv.children.length);

    // Sumamos los nodos como opciones en el select element
    cy.nodes().forEach((node) => {
        createOption(node.id(), selectNodes);
        if (selectNodes.options.length > 0) {
            selectNodes.value = selectNodes.options[0].value;
        }
    })

    // Asociamos un boton a los select para aplicar los cambios.
    attributeButton.addEventListener('click', (event) => {
        let filterAttributes = getFilters(attributeDiv);
        let nodesOptions = filterNodes(cy, filterAttributes);
        selectNodes.innerHTML = '';
        nodesOptions.forEach((node) => {
            createOption(node, selectNodes);
        })
    })

    // Lee el input y filtra los nodos segun corresponda.
    inputNode.addEventListener('input', (event) => {
        let inputValue = event.target.value;
        updateOptions(inputValue, selectNodes);
    })

    // Al hacer click genera la visualizacion del grafo.
    nodeButton.addEventListener('click', (event) => {
        // Obtiene una lista con los nodos seleccionados.
        let initialNode = Array.from(selectNodes.selectedOptions).map(option => option.value);
        // Si selecciono un nodo al menos
        if (initialNode.length !== 0) {
            // Estima fechas si corresponde
            guessDate(cy, initialNode);
            // Devuelve todos los nodos y edges asociados a initialNode
            let {nodes, edges} = findAllNodes(cy, initialNode);
            // Asigna propiedades para visualizarlo en la red
            nodes = assignProperties(nodes);
            // Crea la red
            createNetwork(nodes, edges, options, networkDiv);
        } else {
            alert('No se seleccionaron nodos.');
        }
    })
}

const data = require('./data_v2.js');
drawViz(data);
// dscc.subscribeToData(drawViz, {transform: dscc.tableTransform});