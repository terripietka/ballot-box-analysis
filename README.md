[![Code of Conduct](https://img.shields.io/badge/%E2%9D%A4-code%20of%20conduct-blue.svg?style=flat)](./CODE_OF_CONDUCT.md)

# Ballot Box Analysis

This Python library contains reusable utilities for evaluating the physical placement of ballot drop boxes, given the primary addresses of registered voters. It leverages the [U.S. Census Geocoder](https://geocoding.geo.census.gov/geocoder/) and [Google Geocoding API](https://developers.google.com/maps/documentation/geocoding/overview) to encode each address as a set of latitude/longitude coordinates (as needed), the [TravelTime Isochrones API](https://docs.traveltime.com/api/reference/isochrones) to determine which voters fall within a fixed travel time of each box, and [Kepler.gl](https://kepler.gl/) to visualize findings on an interactive map. Please note some of these services are only available commercially.

To date, these utilities have been used to help a growing number of counties in the Pacific Northwest better understand how the placement of their existing boxes impacts access. Some have taken this a step further, using the same methods to most optimally place additional boxes.

## Setup & Installation

The library can be installed using `pip` or any other equivalent:

```shell
pip install git+https://github.com/usdigitalresponse/ballot-box-analysis.git
```

Where applicable, certain methods expect the following environment variables to be set:

-   `TRAVELTIME_ID`
-   `TRAVELTIME_KEY`
-   `GOOGLE_API_KEY`

## Developing Locally

First, follow [these installation instructions](https://code.visualstudio.com/docs/devcontainers/containers#_installation) to prepare for local development inside a container.

Next, with the "Dev Containers" extension enabled, open the folder containing this repository inside Visual Studio Code.

You should receive a prompt in the Visual Studio Code window. Click the "Reopen in Container" button to run the development environment inside a container.

If you do not receive a prompt:

1. press <kbd>Ctrl</kbd>/<kbd>Cmd</kbd> + <kbd>Shift</kbd> + <kbd>P</kbd> to bring up the command palette in Visual Studio Code; and
2. select the "Dev Containers: Open Folder in Container" command from the palette.

## Code of Conduct

This repository falls under [U.S. Digital Response’s Code of Conduct](./CODE_OF_CONDUCT.md), and we will hold all participants in issues, pull requests, discussions, and other spaces related to this project to that Code of Conduct. Please see [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) for the full code.

## Contributing

This project wouldn’t exist without the hard work of many people. Thanks to the following for all their contributions! Please see [`CONTRIBUTING.md`](./CONTRIBUTING.md) to find out how you can help.

**Lead Maintainer:** [@tyler-richardett](https://github.com/tyler-richardett)

## License & Copyright

Copyright (C) 2025 U.S. Digital Response (USDR)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at:

[`LICENSE`](./LICENSE) in this repository or http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
