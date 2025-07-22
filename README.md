# Mixture Density Network Training for KubeTwin

This repository contains resources for the training Mixture Density Networks (MDNs) on performance traces collected from a Kubernetes microservice environment.  
This codes provides both the tools to extract metrics from a given Kubernetes cluster and to fullfill the training of a MDN capable of reproducing the behaviour of said cluster.

---

## Project Structure

SRC: 
- data_manage contains code to clean data before using it to train MDNs
- metrics retrieval stores a set of functions used in metrics extraction
- models contains every function used to define and train MDNs
- train stores the training core definition
---

## Quick Start

> Training MDN for a single service:

```bash
python src/train/train.py "service_name" data/clean_data.csv
```

> Train all services at once:

```bash
python scripts/train_all.py data/clean_data.csv
```

> Collect traces via HTTP injection and extract from Jaeger:

```bash
python scripts/generate_requests.py -r 5 -n 300 -s 3 -d 5 -u http://127.0.0.1:8000/
```

---

## ⚙️ Features

- Modular MDN architecture (Keras + TFP)
- Weibull loss for latency modeling
- Tracing via Jaeger APIs
- SSH tunneling compatible

---

## Dependencies

Install with:

```bash
pip install -r requirements.txt
```

> TensorFlow 2.15 is mandatory.  
> Python 3.10+ (tested on 3.11.0rc2)

---

## Datasets

Traces are expected in CSV format.  
Store your `.csv` files in the `data/` folder or link to your own storage path.

Each dataset should include:

- `rps_eff`, `rep`: Input features
- `proc_time_<service>`: Target output (in seconds)

The "sample_dataset" file contains an example of how traces should look like if extracted via "generate_requests.py"

---

## Model Output

Trained models are saved in:

```
saved_models/<service_name>/model/

```

Outputs should include:

- `service_train.csv`, `service_test.csv` (split sets)
- `model.keras` weights (saved in Keras format)

---

## Final Note

This code is far from perfect, many of the solutions adopted may be under optimized and clanky.
This project is a research-oriented effort and **not developed by a professional software engineer**.  
Improvements, refactor suggestions, and well-justified pull requests are more than welcome!  
Feel free to open issues or contact me directly if you find something unclear or want to contribute.

> Contact: [needlestream44@gmail.com](mailto:needlestream44@gmail.com)

---

## License

MIT License (see `LICENSE` file)