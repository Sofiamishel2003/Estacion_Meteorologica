# Documentacion

## Correr Pub/Sub normal
Dado que se tienen la direccion del BOOTSTRAP_SERVER y un topico elegido, insertarlos en las funciones main de los archivos Visualizador.py y Simulador_Sensores.py

- Correr Consumidor:
    ```
        python Visualizador.py
    ```
    Esto escribe en consola el estado de los datos recibidos, asi como tambien inica una figura de PLT donde los datos y graficas se actualizen conforme llegan los mensajes

- Correr Productor
    ```
        python Simulador_Sensores.py
    ```
    Este solo corre en terminal, y cada intervalo de tiempo (elegido al azar entre 15 y 30 segundos) se envia un mensaje y se imprime su contenido asi como tambien el tiempo de espera para el siguiente.

## Correr Pub/Sub comprimido
Basandonos en el codigo de las partes anteriores, este archivo comprende las clases de consumidores y productores, para que las funciones encode y decode existan en el mismo contexto y sean mas faciles de debug.
- Correr Pub
    ```
        python payload_restringido.py producer
    ```
- Correr Sub
    ```
        python payload_restringido.py consumer
    ```
Ambos imprimen su contenido en consola. El productor indica los valores generados y la version compresa en 3 bytes en formato hex. Asimismo, el consumidor muestra el contenido recibido en consola e igual que su version pasada visualiza el contenido que vaya recibiendo.