const fs = require('fs');
const readline = require('readline');
const os = require('os');
const path = require('path');
const { createReadStream, createWriteStream, unlinkSync } = require('fs');
const { createInterface } = require('readline');

const CHUNK_SIZE = 500 * 1024 * 1024; // 500 MB
const INPUT_FILE = 'data.txt';
const TEMP_DIR = 'temp_chunks';

if (!fs.existsSync(TEMP_DIR)) {
    fs.mkdirSync(TEMP_DIR);
}

let chunkIndex = 0;
let lines = [];
let currentSize = 0;

const rl = readline.createInterface({
    input: fs.createReadStream(INPUT_FILE),
    crlfDelay: Infinity
});

rl.on('line', (line) => {
    lines.push(line);
    currentSize += Buffer.byteLength(line, 'utf8') + os.EOL.length;

    if (currentSize >= CHUNK_SIZE) {
        lines.sort();
        fs.writeFileSync(path.join(TEMP_DIR, `chunk_${chunkIndex}.txt`), lines.join(os.EOL));
        chunkIndex++;
        lines = [];
        currentSize = 0;
    }
});

rl.on('close', () => {
    if (lines.length > 0) {
        lines.sort();
        fs.writeFileSync(path.join(TEMP_DIR, `chunk_${chunkIndex}.txt`), lines.join(os.EOL));
    }
    console.log('Splitting and sorting complete');
    mergeChunks(chunkIndex + 1);
});

async function mergeChunks(numChunks) {
    const outputFile = 'sorted_large_file.txt';
    const outputStream = createWriteStream(outputFile);
    const streams = [];
    const readers = [];

    for (let i = 0; i < numChunks; i++) {
        const stream = createReadStream(path.join(TEMP_DIR, `chunk_${i}.txt`));
        streams.push(stream);
        const reader = createInterface({ input: stream, crlfDelay: Infinity });
        readers.push(reader[Symbol.asyncIterator]());
    }

    const heap = new MinHeap((a, b) => a.line.localeCompare(b.line));
    
    for (let i = 0; i < readers.length; i++) {
        const { value, done } = await readers[i].next();
        if (!done) {
            heap.insert({ line: value, index: i });
        }
    }

    while (heap.size() > 0) {
        const { line, index } = heap.extract();
        outputStream.write(line + os.EOL);
        const { value, done } = await readers[index].next();
        if (!done) {
            heap.insert({ line: value, index: index });
        }
    }

    outputStream.end(() => {
        console.log('Merging complete');
        cleanupTempFiles(numChunks);
    });
}

function cleanupTempFiles(numChunks) {
    for (let i = 0; i < numChunks; i++) {
        unlinkSync(path.join(TEMP_DIR, `chunk_${i}.txt`));
    }
    fs.rmdirSync(TEMP_DIR);
    console.log('Temporary files cleaned up');
}

class MinHeap {
    constructor(compare) {
        this.compare = compare;
        this.heap = [];
    }

    insert(element) {
        this.heap.push(element);
        this._heapifyUp(this.heap.length - 1);
    }

    extract() {
        if (this.heap.length === 1) {
            return this.heap.pop();
        }
        const top = this.heap[0];
        this.heap[0] = this.heap.pop();
        this._heapifyDown(0);
        return top;
    }

    size() {
        return this.heap.length;
    }

    _heapifyUp(index) {
        let parentIndex = Math.floor((index - 1) / 2);
        while (index > 0 && this.compare(this.heap[index], this.heap[parentIndex]) < 0) {
            [this.heap[index], this.heap[parentIndex]] = [this.heap[parentIndex], this.heap[index]];
            index = parentIndex;
            parentIndex = Math.floor((index - 1) / 2);
        }
    }

    _heapifyDown(index) {
        let smallest = index;
        const left = 2 * index + 1;
        const right = 2 * index + 2;

        if (left < this.heap.length && this.compare(this.heap[left], this.heap[smallest]) < 0) {
            smallest = left;
        }
        if (right < this.heap.length && this.compare(this.heap[right], this.heap[smallest]) < 0) {
            smallest = right;
        }
        if (smallest !== index) {
            [this.heap[index], this.heap[smallest]] = [this.heap[smallest], this.heap[index]];
            this._heapifyDown(smallest);
        }
    }
}
