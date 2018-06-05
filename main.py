#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mpi4py import MPI
import sys
import re

class Proyecto():

    def __init__(self):

        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank() #Proceso actua
        self.size = self.comm.Get_size() #Tamaño de procesos (nodos)
        self.name = MPI.Get_processor_name() #Nombre del procesador (PIV o PD)
        self.next = (self.rank + self.size + 1) % self.size
        self.prev = (self.rank + self.size - 1) % self.size

    def faseUno(self):

        # Inicializar las variables y abrir los archivos
        if self.rank == 0:
            self.libro = open('/home/public/txts/libro_medicina.txt', 'rb').read().decode("utf-8-sig").encode("utf-8")
            palabras = open('/home/public/txts/modificado/palabras_libro_medicina.txt', 'rb')

            self.nodosPalabras = self.generaLista(palabras)
        else:
            self.nodosPalabras = None
            self.libro = None

        # Divide las palabras entre los nodos y le envia el libro a todos los nodos
        self.nodosPalabras = self.comm.scatter(self.nodosPalabras, root=0)
        self.libro = self.comm.bcast(self.libro, root=0)
        palabrasContadas = None

        # Todos los nodos esclavos cuentan las palabras que le corresponden
        if self.rank != 0:
            palabrasContadas = self.contarPalabras()

        # Se envian todas las palabras al coordinador
        palabrasContadas = self.comm.gather(palabrasContadas, root=0)

        if self.rank == 0:
            self.finalizarFaseUno(palabrasContadas)

    def faseDos(self):

        # Recibir del nodo anterior (si no soy 1)
        if self.rank != 1:
            self.libro = self.comm.recv(source=self.prev)

        # Sustituyo mis palabras (si no soy 0)
        if self.rank != 0:

            for palabra in self.nodosPalabras:
                #self.libro.replace(palabra[0], "{" + palabra[1] + "}", 1)
                self.libro = re.sub(r"\b{}\b".format(palabra[0]), "{" + palabra[1] + "}", 
                                     self.libro, 1, re.IGNORECASE|re.MULTILINE)

            # Envio al nodo siguiente (si no soy 0)
            self.comm.send(self.libro, self.next)

        # Crear el archivo del libro (si soy 0)
        if self.rank == 0:

            archivo = open('/home/group/distribuidos/201825_25789/15/faseDos.txt', 'w+')
            archivo.write(self.libro)
            archivo.close()

    # Metodo para que el coordinador termine la fase uno
    def finalizarFaseUno(self, palabrasContadas):
        palabrasAux = []

        # Concatenamos todas las palabras usando una lista auxiliar 
        for palabraNodo in palabrasContadas:
            if palabraNodo != None:
                palabrasAux.extend(palabraNodo)

        # Ordenar las palabras
        palabrasContadas = sorted(palabrasAux, key=lambda x: x[0])

        archivo = open('/home/group/distribuidos/201825_25789/15/faseUno.txt', 'w+')

        for elemento in palabrasContadas:
            archivo.write(elemento[0] + ": " + elemento[1] + "\n")

        archivo.close()


    # Metodo para que los nodos cuenten palabras
    def contarPalabras(self):

        respuesta = list()

        for palabra in self.nodosPalabras:
            respuesta.append((palabra[0], str(self.libro.lower().count(palabra[0].lower()))))

        return respuesta

    # Preparar la estructura para hacer el scatter
    def crearEstructura(self):

        nodos = list()

        for nodo in range(self.size):
            nodos.append(list())

        return nodos

    # Generar lista de palabras
    def generaLista(self, palabras):

        nodos = self.crearEstructura()
        i = 0
        
        for linea in palabras:
            lineaAux = linea.decode("utf-8-sig").encode("utf-8").strip().split(" ", 1)
            lineaAux[1] = lineaAux[1].replace('"', "").replace("'", "")
            nodos[i].append((lineaAux[0].lower(), lineaAux[1].lower()))

            i += 1
            if i >= self.size:
                i = 0

        return nodos

if __name__ == '__main__':
    #proyecto = Proyecto()
    #proyecto.faseUno()
    #proyecto.faseDos()
    proyecto = Proyecto()
    if proyecto.rank == 0:
        tiempoInicial = MPI.Wtime()
    proyecto.faseUno()
    if proyecto.rank == 0:
    	tiempoFinalFaseUno = MPI.Wtime()
    proyecto.faseDos()
    if proyecto.rank == 0:
        tiempoFinal = MPI.Wtime()
        print("Tiempo total transcurrido: " + str(tiempoFinal - tiempoInicial) + " segundos")
	print("Duracion fase uno " + str(tiempoFinalFaseUno - tiempoInicial) + " segundos")
	print("Duracion fase dos " + str(tiempoFinal - tiempoFinalFaseUno) + " segundos")
distribuidos.py
Mostrando distribuidos.py
