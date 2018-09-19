# claimskg_generator
The data lifting module for ClaimsKG that creates the RDF model instantiation from the model 

### Installation & Requirements

Python 3.x

To install the dependencies please use: `pip3 install -r requirements.txt`

### Command-line usage
- Get help use  [under implementation]
```shell
    python3 export.py -h
```
- Exporting a RDF
```shell
    python3 export.py --input data.zip
```
- Using a diferent output name
```shell
    python3 export.py --input data.zip --output out.rdf
```
- Using different a format
```shell
    python3 Exporter.py --input data.zip --format turtle
```
