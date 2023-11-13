import time
import re
import numpy as np

import dask.array as da

import pennylane as qml

import qiskit
from qiskit.quantum_info import Operator

import qulacs
from qulacs.converter import convert_QASM_to_qulacs_circuit

import qibo

import qrisp

import cirq
from cirq.contrib.qasm_import import circuit_from_qasm


from typing import Dict


def calculate_n_qubits_from_qasm(qasm_string):
    return int(
        re.findall(r"qreg q\[(?P<n_qubits>\d*)\]", qasm_string)[0]
    )  # TODO: improvement wanted


class test_fw:
    time_const = 1e-9
    constant_sleep = True
    load = True

    def __init__(self, qasm_circuit, n_shots):
        self.n_qubits = calculate_n_qubits_from_qasm(qasm_circuit)

        self.depth = int(
            qasm_circuit[qasm_circuit.find("\nqreg q[") + 8]
        )  # TODO: improvement wanted

        self.shots = n_shots

    def execute(self) -> None:
        if self.constant_sleep:
            if self.load:
                # Following https://tutorial.dask.org/02_array.html#Dask-array-version
                xd = da.random.normal(10, 0.1, size=(300, 300), chunks=(30, 30))
                yd = xd.mean(axis=0)
                yd.compute()
            else:
                time.sleep(0.01)
        else:
            time.sleep(
                self.time_const * self.shots * self.depth**2 * self.n_qubits**3
            )

    def get_result(self) -> Dict[str, float]:
        counts = {}

        for i in range(2**self.n_qubits):
            bitstring = format(i, f"0{self.n_qubits}b")
            counts[bitstring] = 0

        return counts


class pennylane_fw:
    def __init__(self, qasm_circuit, n_shots):
        self.n_qubits = calculate_n_qubits_from_qasm(qasm_circuit)

        self.n_shots = n_shots

        if self.n_qubits <= 20:
            self.backend = qml.device(
                "default.qubit", wires=range(self.n_qubits), shots=self.n_shots
            )
        else:  # recommended to be used for > 20 qubits
            self.backend = qml.device(
                "lightning.qubit", wires=range(self.n_qubits), shots=self.n_shots
            )

        # Pennylane does not support measurements at the moment
        qasm_circuit_wo_measurement = re.sub(r"measure.*;\n", "", qasm_circuit)
        self.qml_qasm = qml.from_qasm(qasm_circuit_wo_measurement)

        @qml.qnode(self.backend)
        def circuit():
            self.qml_qasm()
            return qml.counts()

        self.qc = circuit

    def execute(self) -> None:
        self.result = self.qc()

    def get_result(self) -> Dict[str, float]:
        counts = self.result

        for i in range(2**self.n_qubits):
            bitstring = format(i, f"0{self.n_qubits}b")
            if bitstring not in counts.keys():
                counts[bitstring] = 0

            else:
                counts[bitstring] = counts[bitstring] / self.n_shots

        return counts


class qiskit_fw:
    def __init__(self, qasm_circuit, n_shots):
        # self.backend = qiskit.Aer.get_backend("qasm_simulator")
        self.backend = qiskit.Aer.get_backend("aer_simulator")

        self.n_qubits = calculate_n_qubits_from_qasm(qasm_circuit)

        self.qc = qiskit.QuantumCircuit.from_qasm_str(qasm_circuit)
        self.n_shots = n_shots
        self.result = None

    def execute(self) -> None:
        self.result = qiskit.execute(self.qc, backend=self.backend, shots=self.n_shots)

    def get_result(self) -> Dict[str, float]:
        counts = self.result.result().get_counts()

        for i in range(2**self.n_qubits):
            bitstring = format(i, f"0{self.n_qubits}b")
            if bitstring not in counts.keys():
                counts[bitstring] = 0

        return counts


class qrisp_fw:
    def __init__(self, qasm_circuit, n_shots):
        self.n_qubits = calculate_n_qubits_from_qasm(qasm_circuit)

        self.qc = qrisp.QuantumCircuit.from_qasm_str(qasm_circuit)
        self.n_shots = n_shots
        self.result = None

    def execute(self) -> None:
        self.result = self.qc.run(shots=self.n_shots)

    def get_result(self) -> Dict[str, float]:
        counts = self.result

        for i in range(2**self.n_qubits):
            bitstring = format(i, f"0{self.n_qubits}b")
            if bitstring not in counts.keys():
                counts[bitstring] = 0

        return counts


class numpy_fw:
    def __init__(self, qasm_circuit, n_shots):
        self.n_shots = n_shots
        cicruit = qiskit.QuantumCircuit.from_qasm_str(qasm_circuit)
        cicruit.remove_final_measurements()
        self.n_qubits = calculate_n_qubits_from_qasm(qasm_circuit)
        self.qc = Operator(cicruit)

    def execute(self) -> None:
        # keine circuits sondern fertige Matrizen
        statevector = np.array(self.qc)[:, 0]
        probabilities = np.abs((statevector) ** 2)
        if self.n_shots is not None:
            self.result = np.random.choice(
                len(probabilities), self.n_shots, p=probabilities
            )
        else:
            self.result = probabilities

    def get_result(self) -> Dict[str, float]:
        counts = {}

        # TODO verification needed!
        for i in range(2**self.n_qubits):
            bitstring = format(i, f"0{self.n_qubits}b")
            c = np.count_nonzero(self.result == i)
            counts[bitstring] = c

        return counts


class cirq_fw:
    def __init__(self, qasm_circuit, n_shots):
        self.n_shots = n_shots

        self.n_qubits = calculate_n_qubits_from_qasm(qasm_circuit)

        self.qc = circuit_from_qasm(qasm_circuit)

        self.qc.append(
            cirq.measure(cirq.NamedQubit.range(self.n_qubits, prefix=""), key="result")
        )
        self.backend = cirq.Simulator()

    def execute(self) -> None:
        if self.n_shots is None:
            self.result = self.backend.simulate(self.qc)
        else:
            self.result = self.backend.run(self.qc, repetitions=self.n_shots)

    def get_result(self) -> Dict[str, float]:
        counts = {}

        # TODO verification needed!
        for i in range(2**self.n_qubits):
            bitstring = format(i, f"0{self.n_qubits}b")

            mask = None
            for j in range(self.n_qubits):
                if mask is None:
                    mask = self.result.data[f"c_{j}"] == int(bitstring[j])
                else:
                    mask &= self.result.data[f"c_{j}"] == int(bitstring[j])
            result = len(self.result.data[mask])

            counts[bitstring] = result / self.n_shots

        return counts


class qibo_fw:
    def __init__(self, qasm_circuit, n_shots):
        qibo.set_backend("numpy")
        self.backend = qibo.get_backend()
        self.n_shots = n_shots

        self.n_qubits = calculate_n_qubits_from_qasm(qasm_circuit)

        # this is super hacky, but the way qibo parses the QASM string
        # does not deserve better.
        def qasm_conv(match: re.Match) -> str:
            denominator = float(match.group()[1:])
            return f"*{1/denominator}"

        qasm_circuit = re.sub(r"/\d*", qasm_conv, qasm_circuit, flags=re.MULTILINE)

        self.qc = qibo.models.Circuit.from_qasm(qasm_circuit)

    def execute(self) -> None:
        self.result = self.qc(nshots=self.n_shots)

    def get_result(self) -> Dict[str, float]:
        counts = dict(self.result.frequencies(binary=True))

        # TODO verification needed!
        for i in range(2**self.n_qubits):
            bitstring = format(i, f"0{self.n_qubits}b")
            if bitstring not in counts.keys():
                counts[bitstring] = 0

        return counts


class qulacs_fw:
    def __init__(self, qasm_circuit, n_shots):
        self.n_qubits = calculate_n_qubits_from_qasm(qasm_circuit)
        qasm_circuit = re.sub("\\nmeasure .*;", "", qasm_circuit)
        qasm_circuit = re.sub("\\ncreg .*;", "", qasm_circuit)
        self.qc = self.convert_QASM_to_qulacs_circuit(qasm_circuit.split("\n"))
        self.n_shots = n_shots
        self.result = None

    def execute(self) -> None:
        state = qulacs.QuantumState(self.n_qubits)
        self.qc.update_quantum_state(state)
        self.result = state.sampling(sampling_count=self.n_shots)

    def get_result(self) -> Dict[str, float]:
        counts = {}
        for i in range(2**self.n_qubits):
            bitstring = format(i, f"0{self.n_qubits}b")
            if i not in self.result:
                counts[bitstring] = 0
            else:
                counts[bitstring] = sum(c == i for c in self.result)

        return counts


# class duration_real(duration_qiskit):
#     def __init__(self, *args, **kwargs):
#         # reading token for interfacing ibm qcs
#         import ibmq_access

#         print(
#             f"Signing in to IBMQ using token {ibmq_access.token[:10]}****, hub {ibmq_access.hub}, group {ibmq_access.group} and project {ibmq_access.project}"
#         )
#         self.provider = q.IBMQ.enable_account(
#             token=ibmq_access.token,
#             hub=ibmq_access.hub,
#             group=ibmq_access.group,
#             project=ibmq_access.project,
#         )
#         self.backend = self.provider.get_backend(config.real_backend)

#         super().__init__(*args, **kwargs)

#     def generate_circuit(self, shots):
#         if self.consistent_circuit == False:
#             self._generate_qiskit_circuit(shots)
#         else:
#             self.qcs = []

#             if shots is None:
#                 return  # just return in case no shots are specified

#             for e in range(self.evals):
#                 # welchen Wert haben die qubits am Anfang?
#                 qasm_circuit = utils.get_random_qasm_circuit(
#                     self.qubits, self.depth, self.seed
#                 )
#                 qc = q.QuantumCircuit.from_qasm_str(qasm_circuit)
#                 # warum wird hier schon gemessen?
#                 self.qcs.append(qc)

#     def _generate_qiskit_circuit(self, shots):
#         self.qcs = []

#         if shots is None:
#             return  # just return in case no shots are specified

#         for e in range(self.evals):
#             # welchen Wert haben die qubits am Anfang?
#             qc = random_circuit(
#                 self.qubits, self.depth, max_operands=3, measure=True, seed=self.seed
#             )
#             # warum wird hier schon gemessen?
#             self.qcs.append(qc)

#     def execute(self, shots):
#         if self.qcs.__len__() == 0:
#             return 0
#         result = q.execute(self.qcs, backend=self.backend, shots=shots).result()

#         duration = result._metadata["time_taken"]

#         return duration

#     def time_measurement(self, shots):
#         return self.execute(shots)
