# DNAContainer
This is the code project of the paper [DNAContainer](https://dl.gi.de/handle/20.500.12116/40358?locale-attribute=en). This is an interface for interacting with DNA storage.

## Installation
Make sure you have [Java 20+](https://www.oracle.com/de/java/technologies/downloads/) and [Maven](https://maven.apache.org/download.cgi) installed. Then, you can build the executable _.jar_ file with `mvn package`.

## Parameters
All parameters are set in the JSON config file `params.ini` that must be located in the same directory as the executable _.jar_ file. There is an example `params.ini` in this project that can be used to encode a CSV file. Each of the parameters is well documented in the `params.ini` file.

## Usage
The program requires setting the correct parameters to start generating. Parameters can be set in the `params.ini` file in the same directory of the _.jar_ file. The parameters are parsed into the program by a JSON parser. See the following examples for usage.

### Example
Use the following command from the command line (or shell) from the `target` directory:
```sh
java -jar DNAContainer-1.0-full.jar params.ini
```
The command above will insert every line of the CSV file specified in the parameters' file `params.ini` into DNAContainer.
