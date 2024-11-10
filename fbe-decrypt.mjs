// BSD Zero Clause License
// 
// Copyright (c) 2024 SlugFiller
// 
// Permission to use, copy, modify, and/or distribute this software for any
// purpose with or without fee is hereby granted.
// 
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
// REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
// INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
// LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
// OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
// PERFORMANCE OF THIS SOFTWARE.

import { open as fopen } from 'node:fs/promises';
import { dirname, join as pathjoin } from 'node:path';
import { createDecipheriv, createHmac, createHash } from 'node:crypto';

async function *using(resource) {
	try {
		yield resource;
	}
	finally {
		resource && resource.close && await resource.close();
	}
}

function* range(start, end) {
	for (let i = start; i < end; i++) {
		yield i;
	}
}

function binarySearch(length, condition) {
	// Returns the first non-negative integer i where condition(i) is true
	// Assuming condition is monotonic "rising" from false to true
	// Returns length if the condition is false for all i from 0 through length - 1
	let l = 0;
	let h = length;
	while (l < h) {
		const m = (l + h) >>> 1;
		if (condition(m)) {
			h = m;
		}
		else {
			l = m + 1;
		}
	}
	return h;
}

async function binarySearchAsync(length, condition) {
	// Same as above, but condition is async
	let l = 0;
	let h = length;
	while (l < h) {
		const m = (l + h) >>> 1;
		if (await condition(m)) {
			h = m;
		}
		else {
			l = m + 1;
		}
	}
	return h;
}

function binarySearchArray(array, condition) {
	return binarySearch(array.length, (i) => condition(array[i]));
}

async function iteratorToArrayAsync(iterator, limit) {
	const ret = [];
	for await (const entry of iterator) {
		ret.push(entry);
		if (limit) {
			limit--;
			if (limit < 1) {
				break;
			}
		}
	}
	return ret;
}

class FileHandleBlockDev {
	#dev;
	#blocksize;
	#lastreadblock;
	#lastreadblockoffset;
	#stats;

	constructor(dev) {
		this.#dev = dev;
		this.#lastreadblockoffset = -1n;
		this.#stats = {
			size: dev.blockCount * BigInt(dev.blockSize)
		};
	}

	async read(buffer, offset, length, position, ...options) {
		position = BigInt(position);
		let buffer_offset = 0;
		const srcblocksize = this.#dev.blockSize;
		let srcblock = position / BigInt(srcblocksize);
		const firstblock = this.#lastreadblockoffset === srcblock?this.#lastreadblock:await this.#dev.readBlock(srcblock, ...options);
		const firstoffset = Number(position % BigInt(srcblocksize));
		if (firstoffset + length <= srcblocksize) {
			this.#lastreadblock = firstblock;
			this.#lastreadblockoffset = srcblock;
			firstblock.copy(buffer, offset, firstoffset, firstoffset + length);
			return length;
		}
		firstblock.copy(buffer, 0, firstoffset, srcblocksize);
		offset = srcblocksize - firstoffset;
		const end = offset + length;
		while (offset + srcblocksize < end) {
			srcblock++;
			(await this.#dev.readBlock(srcblock, ...options)).copy(buffer, offset, 0, srcblocksize);
			offset += srcblocksize;
		}
		srcblock++;
		(this.#lastreadblock = await this.#dev.readBlock(this.#lastreadblockoffset = srcblock, ...options)).copy(buffer, offset, 0, end - offset);
		return length;
	}

	async stat() {
		return this.#stats;
	}
}

class FileHandleBufferIterator {
	#iterator;
	#iteration;
	#buffer;
	#position;
	#stats;

	static async open(iterator, total_size) {
		const self = new FileHandleBufferIterator();
		self.#stats = {
			size: BigInt(total_size)
		};
		self.#iterator = iterator;
		self.#iteration = iterator();
		self.#position = 0n;
		self.#buffer = null;
		const {done: done, value: value} = await self.#iteration.next();
		if (!done) {
			self.#buffer = value;
		}
		return self;
	}

	async read(buffer, offset, length, position) {
		position = BigInt(position);
		if (position < this.#position) {
			await this.#iteration.return();
			this.#position = 0n;
			this.#buffer = null;
			this.#iteration = this.#iterator();
			const {done: done, value: value} = await this.#iteration.next();
			if (!done) {
				this.#buffer = value;
			}
		}
		if (this.#buffer === null) {
			return 0;
		}
		while (this.#position + BigInt(this.#buffer.length) <= position) {
			this.#position += BigInt(this.#buffer.length);
			this.#buffer = null;
			const {done: done, value: value} = await this.#iteration.next();
			if (done) {
				return 0;
			}
			this.#buffer = value;
		}
		if (position + BigInt(length) <= this.#position + BigInt(this.#buffer.length)) {
			this.#buffer.copy(buffer, offset, Number(position - this.#position), Number(position - this.#position) + length);
			return length;
		}
		this.#buffer.copy(buffer, offset, Number(position - this.#position), this.#buffer.length);
		let ret = this.#buffer.length - Number(position - this.#position);
		offset += ret;
		length -= ret;
		while (true) {
			this.#position += BigInt(this.#buffer.length);
			this.#buffer = null;
			const {done: done, value: value} = await this.#iteration.next();
			if (done) {
				return ret;
			}
			this.#buffer = value;
			if (length <= this.#buffer.length) {
				this.#buffer.copy(buffer, offset, 0, length);
				return length;
			}
			this.#buffer.copy(buffer, offset, 0, this.#buffer.length);
			ret += this.#buffer.length;
			offset += this.#buffer.length;
			length -= this.#buffer.length;
		}
	}

	async stat() {
		return this.#stats;
	}

	async snapshotAt(position, limit) {
		const nulliterator = {
			open: async () => {
				return await FileHandleBufferIterator.open(async function *() {}, 0n);
			}
		};
		position = BigInt(position);
		const maxsize = limit?BigInt(limit) - position:false;
		if (position < this.#position) {
			await this.#iteration.return();
			this.#position = 0n;
			this.#buffer = null;
			this.#iteration = this.#iterator();
			const {done: done, value: value} = await this.#iteration.next();
			if (!done) {
				this.#buffer = value;
			}
		}
		if (this.#buffer === null) {
			return nulliterator;
		}
		while (this.#position + BigInt(this.#buffer.length) <= position) {
			this.#position += BigInt(this.#buffer.length);
			this.#buffer = null;
			const {done: done, value: value} = await this.#iteration.next();
			if (done) {
				return nulliterator;
			}
			this.#buffer = value;
		}
		const {done: done, value: value} = await this.#iteration.next(true);
		if (done) {
			return nulliterator;
		}
		const subbuffer = this.#buffer.subarray(Number(position - this.#position), (maxsize === false || maxsize > BigInt(this.#buffer.length))?this.#buffer.length:Number(position - this.#position + maxsize));
		const size = (maxsize === false || position + maxsize > this.#stats.size)?this.#stats.size - position:maxsize;
		async function *snapshot_remain_iterator(snapshot, remain) {
			if (remain === false) {
				yield *snapshot();
				return;
			}
			const iter = snapshot();
			for await (const buffer of iter) {
				if (BigInt(buffer.length) > remain) {
					if (yield buffer.subarray(0, remain)) {
						while (yield async function *() {});
					}
					break;
				}
				remain -= BigInt(buffer.length);
				if (yield buffer) {
					while (yield snapshot_remain_iterator.bind(null, await iter.next(true), remain));
				}
			}
		}
		async function *snapshot_iterator() {
			if (yield subbuffer) {
				while (yield snapshot_remain_iterator.bind(null, value, (maxsize !== false)?maxsize - BigInt(subbuffer.length):false));
			}
			yield *snapshot_remain_iterator(value, (maxsize !== false)?maxsize - BigInt(subbuffer.length):false);
		}
		return {
			open: async () => {
				return await FileHandleBufferIterator.open(snapshot_iterator, size);
			}
		};
	}

	async close() {
		await this.#iteration.return();
	}
}

class FileHandleBuffer {
	#buffer;

	constructor(buffer) {
		this.#buffer = buffer;
	}

	async read(buffer, offset, length, position) {
		position = BigInt(position);
		if (position + BigInt(length) <= 0n) {
			return 0;
		}
		if (position < 0n) {
			offset -= Number(position);
			length += Number(position);
			position = 0n;
		}
		if (position >= BigInt(this.#buffer.length)) {
			return 0;
		}
		if (position + BigInt(length) > BigInt(this.#buffer.length)) {
			this.#buffer.copy(buffer, offset, Number(position), this.#buffer.length);
			return this.#buffer.length - Number(position);
		}
		this.#buffer.copy(buffer, offset, Number(position), Number(position) + length);
		return length
	}


	async stat() {
		return {
			size: BigInt(this.#buffer.length)
		};
	}
}

class FileSequenceReader {
	#file;
	#position;
	#limit;
	#buffer;

	constructor(file, position, limit) {
		this.#file = file;
		this.#position = position?BigInt(position):0n;
		this.#limit = limit?BigInt(limit):false;
		this.#buffer = Buffer.alloc(8);
	}

	get position() {
		return this.#position;
	}

	set limit(limit) {
		this.#limit = limit?BigInt(limit):false;
	}

	skip(length) {
		this.#position += BigInt(length);
	}

	async read(buffer, offset, length, ...options) {
		if (this.#limit !== false) {
			if (this.#position >= this.#limit) {
				return 0;
			}
			if (this.#position + BigInt(length) > this.#limit) {
				length = Number(this.#limit - this.#position);
			}
		}
		if (length <= 0) {
			return 0;
		}
		const ret = await this.#file.read(buffer, offset, length, Number(this.#position), ...options);
		this.#position += BigInt(ret);
		return ret;
	}

	async isEOF(...options) {
		if (this.#limit !== false && this.#position >= this.#limit) {
			return true;
		}
		return await this.#file.read(this.#buffer, 0, 1, Number(this.#position), ...options) < 1;
	}

	async readInt8(...options) {
		if (await this.read(this.#buffer, 0, 1, ...options) < 1) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readInt8(0);
	}

	async readUInt8(...options) {
		if (await this.read(this.#buffer, 0, 1, ...options) < 1) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readUInt8(0);
	}

	async readInt16LE(...options) {
		if (await this.read(this.#buffer, 0, 2, ...options) < 2) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readInt16LE(0);
	}

	async readInt16BE(...options) {
		if (await this.read(this.#buffer, 0, 2, ...options) < 2) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readInt16BE(0);
	}

	async readUInt16LE(...options) {
		if (await this.read(this.#buffer, 0, 2, ...options) < 2) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readUInt16LE(0);
	}

	async readUInt16BE(...options) {
		if (await this.read(this.#buffer, 0, 2, ...options) < 2) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readUInt16BE(0);
	}

	async readInt32LE(...options) {
		if (await this.read(this.#buffer, 0, 4, ...options) < 4) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readInt32LE(0);
	}

	async readInt32BE(...options) {
		if (await this.read(this.#buffer, 0, 4, ...options) < 4) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readInt32BE(0);
	}

	async readUInt32LE(...options) {
		if (await this.read(this.#buffer, 0, 4, ...options) < 4) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readUInt32LE(0);
	}

	async readUInt32BE(...options) {
		if (await this.read(this.#buffer, 0, 4, ...options) < 4) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readUInt32BE(0);
	}

	async readBigInt64LE(...options) {
		if (await this.read(this.#buffer, 0, 8, ...options) < 8) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readBigInt64LE(0);
	}

	async readBigInt64BE(...options) {
		if (await this.read(this.#buffer, 0, 8, ...options) < 8) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readBigInt64BE(0);
	}

	async readBigUInt64LE(...options) {
		if (await this.read(this.#buffer, 0, 8, ...options) < 8) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readBigUInt64LE(0);
	}

	async readBigUInt64BE(...options) {
		if (await this.read(this.#buffer, 0, 8, ...options) < 8) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readBigUInt64BE(0);
	}

	async readDoubleLE(...options) {
		if (await this.read(this.#buffer, 0, 8, ...options) < 8) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readDoubleLE(0);
	}

	async readDoubleBE(...options) {
		if (await this.read(this.#buffer, 0, 8, ...options) < 8) {
			throw new Error('Unexpected EOF');
		}
		return this.#buffer.readDoubleBE(0);
	}
}

class BlockDevFile {
	#file;
	#blocksize;
	#numblocks;

	static async open(handle, blocksize) {
		const self = new BlockDevFile();
		self.#file = handle;
		self.#blocksize = blocksize;
		const stats = await handle.stat({bigint: true});
		self.#numblocks = (stats.size + BigInt(blocksize - 1)) / BigInt(blocksize);
		return self;
	}

	get blockSize() {
		return this.#blocksize;
	}

	get blockCount() {
		return this.#numblocks;
	}

	async readBlock(offset, ...options) {
		const res = Buffer.alloc(this.#blocksize);
		await this.#file.read(res, 0, this.#blocksize, Number(BigInt(offset) * BigInt(this.#blocksize)), ...options);
		return res;
	}
}

class BlockDevQcow2 {
	#resource;
	#file;
	#image;
	#blocksize;
	#numblocks;
	#l1;
	#l2;
	#l2_offset;
	#snapshots;

	static async open(path) {
		const self = new BlockDevQcow2();
		self.#l2_offset = 0;
		async function *resource() {
			for await (const file of using(await fopen(path))) {
				self.#file = file;
				let header = Buffer.alloc(72);
				await file.read(header, 0, 72, 0);
				if (header.readUInt32BE(0) != 0x514649FB) {
					throw new Error('QCOW magic string mismatch');
				}
				const cluster_bits = header.readUInt32BE(20);
				if (cluster_bits < 9 || cluster_bits > 21) {
					throw new Error('Invalid cluster_bits: ' + cluster_bits);
				}
				if (header.readUInt32BE(32) !== 0) {
					throw new Error('Encryption not supported');
				}
				const version = header.readUInt32BE(4);
				if (version > 2) {
					const header_size_buffer = Buffer.alloc(4);
					await file.read(header_size_buffer, 0, 4, 100);
					const header_size = header_size_buffer.readUInt32BE(0);
					if (header_size < 104 || header_size & 7) {
						throw new Error('Invalid header size: ' + header_size);
					}
					header = Buffer.alloc(header_size);
					await file.read(header, 0, header_size, 0);
					const incompatible_features = header.readBigUInt64BE(72);
					if (incompatible_features & ~3n) {
						// Supported features:
						// Dirty bit (Ignored, refcounts unused)
						// Corrupt bit (Ignored, using as read only)
						// Unsupported known features:
						// Compression type - Used for Zstandard support. However, NodeJS does not have built-in support for Zstandard
						// External data file
						// Extended L2 entries
						throw new Error('Unsupported incompatible features: ' + (incompatible_features & ~3n).toString(16));
					}
				}
				self.#snapshots = [];
				const snapshots_count = header.readUInt32BE(60);
				if (snapshots_count > 0) {
					let position = header.readBigUInt64BE(64);
					const snapshot = Buffer.alloc(40);
					for (const i of range(0, snapshots_count)) {
						await file.read(snapshot, 0, 40, Number(position));
						position += 40n;
						const l1_offset = snapshot.readBigUInt64BE(0);
						const l1_size = snapshot.readUInt32BE(8);
						const unique_id = Buffer.alloc(snapshot.readUInt16BE(12));
						const name = Buffer.alloc(snapshot.readUInt16BE(14));
						position += BigInt(snapshot.readUInt16BE(36));
						await file.read(unique_id, 0, unique_id.length, Number(position));
						position += BigInt(unique_id.length);
						await file.read(name, 0, name.length, Number(position));
						position += BigInt(name.length);
						position += 7n - ((position + 7n) & 7n);
						self.#snapshots.push({
							time: new Date(snapshot.readUInt32BE(16) * 1000),
							time_nanoseconds: snapshot.readUInt32BE(20),
							guest_runtime_nanoseconds: snapshot.readBigUInt64BE(24),
							id: unique_id.toString(),
							name: name.toString(),
							load: async () => {
								self.#l1 = Buffer.alloc(l1_size * 8);
								await file.read(self.#l1, 0, l1_size * 8, Number(l1_offset));
							}
						});
					}
				}
				self.#blocksize = 1 << cluster_bits;
				self.#numblocks = (header.readBigUInt64BE(24) + BigInt(self.#blocksize - 1)) / BigInt(self.#blocksize);
				const l1_size = header.readUInt32BE(36);
				self.#l1 = Buffer.alloc(l1_size * 8);
				await file.read(self.#l1, 0, l1_size * 8, Number(header.readBigUInt64BE(40)));
				const backing_file_offset = header.readBigUInt64BE(8);
				if (backing_file_offset !== 0n) {
					const backing_file_size = header.readUInt32BE(16);
					const backing_file_name = Buffer.alloc(backing_file_size);
					await file.read(backing_file_name, 0, backing_file_size, Number(backing_file_offset));
					let backing_file_type = 'raw';
					const extension_id = Buffer.alloc(8);
					let position = BigInt(header.length);
					while (true) {
						await file.read(extension_id, 0, 8, Number(position));
						const type = extension_id.readUInt32BE(0);
						if (type === 0x00000000) {
							break;
						}
						if (type === 0xe2792aca) {
							const data = Buffer.alloc(extension_id.readUInt32BE(4))
							await file.read(data, 0, data.length, Number(position + 8n));
							backing_file_type = data.toString();
							break;
						}
						position += (BigInt(extension_id.readUInt32BE(4)) + 15n) & ~7n;
					}
					if (backing_file_type === 'qcow2') {
						for await (const backing_file of using(await BlockDevQcow2.open(pathjoin(dirname(path), backing_file_name.toString())))) {
							self.#image = backing_file;
							yield;
							return;
						}
					}
					else {
						for await (const backing_file of using(await fopen(pathjoin(dirname(path), backing_file_name.toString())))) {
							self.#image = await BlockDevFile.open(backing_file, self.#blocksize);
							yield;
							return;
						}
					}
				}
				yield;
			}
		}
		self.#resource = resource();
		await self.#resource.next();
		return self;
	}

	get blockSize() {
		return this.#blocksize;
	}

	get blockCount() {
		return this.#numblocks;
	}

	get snapshots() {
		return this.#snapshots;
	}

	async readBlock(offset) {
		offset = BigInt(offset);
		const res = Buffer.alloc(this.#blocksize);
		const l2_entries = BigInt(this.#blocksize >>> 3);
		const l1_index = (offset / l2_entries) << 3n;
		if (l1_index < 0n || l1_index >= BigInt(this.#l1.length)) {
			return res;
		}
		const l1 = this.#l1.readBigUInt64BE(Number(l1_index)) & 0x00FFFFFFFFFFFFFFn;
		if (l1 === 0n) {
			return (this.#image && await this.#image.readBlock(offset)) || res;
		}
		if (this.#l2_offset !== l1) {
			this.#l2_offset = l1;
			this.#l2 = Buffer.alloc(this.#blocksize);
			await this.#file.read(this.#l2, 0, this.#blocksize, Number(this.#l2_offset));
		}
		const l2 = this.#l2.readBigUInt64BE(Number((offset % l2_entries) << 3n));
		if (l2 === 0n) {
			return (this.#image && await this.#image.readBlock(offset)) || res;
		}
		if (l2 & 1n) {
			return res;
		}
		if (l2 & 0x4000000000000000n) {
			// FIXME: Compressed clusters. Support at least deflate
			return res;
		}
		await this.#file.read(res, 0, this.#blocksize, Number(l2 & 0x00FFFFFFFFFFFFFFn));
		return res;
	}

	async close() {
		await this.#resource.return();
	}
}

class BlockDevSubset {
	#dev;
	#offset;
	#count;

	constructor(dev, offset, count) {
		this.#dev = dev;
		this.#offset = BigInt(offset);
		this.#count = BigInt(count);
	}

	get blockSize() {
		return this.#dev.blockSize;
	}

	get blockCount() {
		return this.#count;
	}

	async readBlock(offset, ...options) {
		offset = BigInt(offset);
		if (offset < 0n || offset >= this.#count) {
			return Buffer.alloc(this.#dev.blockSize);
		}
		return await this.#dev.readBlock(offset + this.#offset, ...options);
	}
}

class BlockDevDecrypt {
	#dev;
	#defaultkey;
	#blankbuffer;

	// FIXME: Also ask for default algorithm, to support Adiantum in addition to AES-256-XTS
	constructor(dev, defaultkey) {
		this.#dev = dev;
		this.#defaultkey = defaultkey;
		this.#blankbuffer = Buffer.alloc(dev.blockSize);
	}

	get blockSize() {
		return this.#dev.blockSize;
	}

	get blockCount() {
		return this.#dev.blockCount;
	}

	// FIXME: Also ask for algorithm, to support Adiantum
	async readBlock(offset, key, fileblock) {
		const block = await this.#dev.readBlock(offset);
		if (block.equals(this.#blankbuffer)) {
			// Special case: Leave all-0 blocks as-is, as these may simply be unallocated blocks
			return block;
		}
		const iv = Buffer.alloc(16);
		iv.writeBigUInt64LE(BigInt(fileblock === undefined?offset:fileblock));
		const block_decipher = createDecipheriv('aes-256-xts', key || this.#defaultkey, iv);
		return Buffer.concat([block_decipher.update(block), block_decipher.final()]);
	}
}

class GPT {
	#partitions;

	static async open(dev) {
		const self = new GPT();
		if (dev.blockSize < 88) {
			throw new Error('Device block size is too small');
		}
		const header = await dev.readBlock(1);
		if (header.readBigUInt64BE(0) !== 0x4546492050415254n) {
			throw new Error('GPT signature not found');
		}
		self.#partitions = [];
		const partitionSize = BigInt(header.readUInt32LE(84));
		if (partitionSize < 128n) {
			throw new Error('Partition descriptor size too small: ' + partitionSize);
		}
		const partition_buffer = Buffer.alloc(128);
		let partition_count = header.readUInt32LE(80);
		if (partition_count <= 0) {
			return self;
		}
		const partition_file = new FileHandleBlockDev(dev);
		let partition_offset = header.readBigUInt64LE(72) * BigInt(dev.blockSize);
		while (partition_count > 0) {
			partition_count--;
			await partition_file.read(partition_buffer, 0, 128, Number(partition_offset));
			partition_offset += partitionSize;
			const first_block = partition_buffer.readBigUInt64LE(32);
			const last_block = partition_buffer.readBigUInt64LE(40);
			if (first_block === 0n || last_block < first_block) {
				continue;
			}
			self.#partitions.push({
				type: guid(partition_buffer.subarray(0, 16)),
				id: guid(partition_buffer.subarray(16, 32)),
				data: new BlockDevSubset(dev, first_block, last_block - first_block + 1n),
				name: partition_buffer.subarray(56).toString('utf-16le').replace(/\x00+$/, '')
			});
		}
		return self;
		function byteToHex(c) {
			return c.toString(16).toUpperCase().padStart(2, '0');
		}
		function guid(buffer) {
			return [3, 2, 1, 0, '-', 5, 4, '-', 7, 6, '-', 8, 9, '-', 10, 11, 12, 13, 14, 15].map(x => x === '-'?'-':byteToHex(buffer[x])).join('');
		}
	}

	get partitions() {
		return this.#partitions;
	}
}

class FileSystemExt4 {
	#file;
	#temp_buffer;
	#blocksize;
	#blocks_per_group;
	#direntry_with_filetype;
	#ea_inode;
	#meta_bg;
	#first_meta_bg;
	#flex_bg;
	#loaded_group_descriptor;
	#group_descriptor;
	#inodes_per_group;
	#inodes_count;
	#inode;
	#loaded_inode;
	#inode_bitmap;
	#loaded_inode_bitmap;
	#indirects;
	#loaded_indirects;
	#last_extent_inode;
	#last_extent_block;
	#last_extent_min;
	#last_extent_max;
	#root;
	#keys;

	static async open(file) {
		const self = new FileSystemExt4();
		self.#keys = new Map();
		self.#file = file;
		self.#loaded_group_descriptor = -1n;
		self.#loaded_inode = -1n;
		self.#loaded_inode_bitmap = -1n;
		self.#indirects = [0n, 0n, 0n];
		self.#loaded_indirects = [-1n, -1n, -1n];
		self.#last_extent_inode = -1n;
		const superblock = Buffer.alloc(1024);
		await file.read(superblock, 0, 1024, 1024);
		if (superblock.readUInt16LE(0x38) !== 0xEF53) {
			throw new Error('Invalid magic signature');
		}
		const log_block_size = superblock.readUInt32LE(0x18);
		if (log_block_size > 6) {
			throw new Error('Block size too large: ' + (log_block_size + 10) + ' bits');
		}
		self.#blocksize = BigInt(1 << (log_block_size + 10));
		self.#blocks_per_group = BigInt(superblock.readUInt32LE(0x20));
		const rev_level = superblock.readUInt32LE(0x4C);
		const inode_size = (rev_level > 0)?superblock.readUInt16LE(0x58):128;
		if (inode_size < 128) {
			throw new Error('inode size too small: ' + inode_size);
		}
		const feature_incompat = (rev_level > 0)?superblock.readUInt32LE(0x60):0;
		if (feature_incompat & ~0x3E7DE) {
			// Supported features:
			// EXT4_FEATURE_INCOMPAT_FILETYPE
			// EXT4_FEATURE_INCOMPAT_RECOVER (Ignored)
			// EXT4_FEATURE_INCOMPAT_JOURNAL_DEV (Ignored)
			// EXT4_FEATURE_INCOMPAT_META_BG
			// EXT4_FEATURE_INCOMPAT_EXTENTS (Assumed)
			// EXT4_FEATURE_INCOMPAT_64BIT
			// EXT4_FEATURE_INCOMPAT_MMP (Ignored)
			// EXT4_FEATURE_INCOMPAT_FLEX_BG
			// EXT4_FEATURE_INCOMPAT_EA_INODE
			// EXT4_FEATURE_INCOMPAT_CSUM_SEED (Ignored)
			// EXT4_FEATURE_INCOMPAT_LARGEDIR (Ignored, linear scans used instead of htree)
			// EXT4_FEATURE_INCOMPAT_INLINE_DATA (Assumed)
			// EXT4_FEATURE_INCOMPAT_ENCRYPT
			// EXT4_FEATURE_INCOMPAT_CASEFOLD (Ignored)
			// Unsupported known features:
			// EXT4_FEATURE_INCOMPAT_COMPRESSION
			// EXT4_FEATURE_INCOMPAT_DIRDATA
			throw new Error('Partition contains unsupported incompatible features: ' + (feature_incompat & ~0x1E7DE).toString(16));
		}
		const is64bit = !!(feature_incompat & 0x80);
		const group_descriptor_size = is64bit?superblock.readUInt16LE(0xFE):32;
		if (is64bit && group_descriptor_size < 32) {
			throw new Error('Group descriptor size too small: ' + group_descriptor_size);
		}
		self.#direntry_with_filetype = !!(feature_incompat & 0x2);
		self.#ea_inode = !!(feature_incompat & 0x400);
		self.#meta_bg = false;
		if (feature_incompat & 0x10) {
			self.#meta_bg = true;
			self.#first_meta_bg = BigInt(superblock.readUInt32LE(0x104));
		}
		self.#flex_bg = (feature_incompat & 0x200)?BigInt(superblock.readUInt8LE(0x174)):0n;
		self.#inodes_per_group = BigInt(superblock.readUInt32LE(0x28)) << self.#flex_bg;
		self.#inodes_count = superblock.readUInt32LE(0);
		self.#group_descriptor = Buffer.alloc(group_descriptor_size);
		self.#inode = Buffer.alloc(inode_size);
		self.#inode_bitmap = Buffer.alloc(1);
		self.#temp_buffer = Buffer.alloc(8);
		if (!await self.#loadInode(2)) {
			throw new Error('Root inode could not be loaded');
		}
		self.#root = await self.#createDirent('/');
		return self;
	}

	get root() {
		return this.#root;
	}

	addKey(key) {
		const hmac1 = createHmac('sha512', '');
		hmac1.update(key);
		const hmac2 = createHmac('sha512', hmac1.digest());
		hmac2.update('fscrypt\0');
		hmac2.update(Buffer.from([1, 1]));	// Context=Key Identifier, Counter=1
		this.#keys.set(hmac2.digest('hex').substr(0, 32), key);
	}

	async *decrypt(target) {
		const temp_buffer = Buffer.alloc(1024);
		const buffer_c = Buffer.from('c');
		for (const inode of range(1, this.#inodes_count + 1)) {
			if (!await this.#loadInode(inode)) {
				continue;
			}
			yield [inode, this.#inodes_count];
			const loaded_inode = this.#loaded_inode;
			const filetype = this.#inode.readUInt16LE(0) & 0xF000;
			const flags = this.#inode.readUInt32LE(0x20);
			if (!(flags & 0x800)) {
				continue;
			}
			const profile = Buffer.alloc(40);
			const res = await this.#readExtendedAttribute(9, buffer_c, profile, 0, profile.length, 0n)
			if (res < 40) {
				continue;
			}
			const masterkey = this.#keys.get(profile.subarray(8, 24).toString('hex'));
			if (!masterkey) {
				continue;
			}
			// Only remove the encryption flag after we confirmed we can decrypt this inode
			temp_buffer.writeUInt32LE(flags & ~0x800, 0);
			target.write(temp_buffer, 0, 4, loaded_inode + 0x20n);
			// Rename the encryption attribute to user.c, so tools don't mistake it for
			// an encrypted inode with a missing flag
			temp_buffer[0] = 1;
			for await (const attribute of this.#iterateAttributes()) {
				if (attribute.index !== 9 || !buffer_c.equals(attribute.name)) {
					continue;
				}
				target.write(temp_buffer, 0, 1, attribute.root + 1n);
			}
			const hmac1 = createHmac('sha512', '');
			hmac1.update(masterkey);
			const hmac2 = createHmac('sha512', hmac1.digest());
			hmac2.update('fscrypt\0');
			hmac2.update(Buffer.from([2]));	// Context=Per-file Encryption Key
			hmac2.update(profile.subarray(24));
			hmac2.update(Buffer.from([1]));	// Counter=1
			const derived_key = hmac2.digest();
			if (filetype === 0xA000) {	// Symlink
				if (this.#loaded_inode !== loaded_inode) {
					this.#loaded_inode = loaded_inode;
					await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
				}
				const first_read = await this.#readFile(temp_buffer, 0, 1024, 0n);
				if (first_read < 1024) {
					if (this.#loaded_inode !== loaded_inode) {
						this.#loaded_inode = loaded_inode;
						await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
					}
					// Sufficiently small link target can be decoded in a single buffer
					// Most symlinks should go through this path
					await this.#writeFile(target, decryptCts(temp_buffer.subarray(0, first_read), derived_key.subarray(0, 32)), 0, first_read, 0n);
					continue;
				}
				let position = 992n;
				let write_position = 0n;
				const decipher = createDecipheriv('aes-256-cbc', derived_key.subarray(0, 32), Buffer.alloc(16));
				decipher.setAutoPadding(false);
				while (true) {
					// Decrypt 32 bytes less than we read, since we may need to adjust
					// those 32 bytes if they are the last 32 bytes
					const plaintext_prefix = decipher.update(temp_buffer.subarray(0, 992))
					if (this.#loaded_inode !== loaded_inode) {
						this.#loaded_inode = loaded_inode;
						await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
					}
					if (await this.#writeFile(target, plaintext_prefix, 0, plaintext_prefix.length, write_position) < plaintext_prefix.length) {
						break;
					}
					write_position += BigInt(plaintext_prefix.length);
					if (this.#loaded_inode !== loaded_inode) {
						this.#loaded_inode = loaded_inode;
						await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
					}
					const next_read = await this.#readFile(temp_buffer, 0, 1024, position);
					if (next_read < 1024) {
						position += BigInt(next_read);
						const cts_cipher = Buffer.alloc(32);
						const swizzle_start = ((next_read + 15) & ~15) - 16;
						temp_buffer.copy(cts_cipher, 0, swizzle_start, next_read);
						temp_buffer.copy(cts_cipher, 16, swizzle_start - 16, swizzle_start);
						if (swizzle_start > 16) {
							const plaintext_infix = decipher.update(temp_buffer.subarray(0, swizzle_start - 16));
							if (this.#loaded_inode !== loaded_inode) {
								this.#loaded_inode = loaded_inode;
								await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
							}
							if (await this.#writeFile(target, plaintext_infix, 0, plaintext_infix.length, write_position) < plaintext_infix.length) {
								break;
							}
							write_position += BigInt(plaintext_infix.length);
						}
						const swizzle_offset = next_read & 15;
						if (swizzle_offset > 0) {
							const ecb = createDecipheriv('aes-256-ecb', derived_key.subarray(0, 32), Buffer.alloc(0));
							ecb.setAutoPadding(false);
							ecb.update(temp_buffer.subarray(swizzle_start - 16, swizzle_start)).copy(cts_cipher, swizzle_offset + 16, swizzle_offset, 16);
						}
						const plaintext_suffix = decipher.update(cts_cipher).subarray(0, Number(position - write_position));
						if (this.#loaded_inode !== loaded_inode) {
							this.#loaded_inode = loaded_inode;
							await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
						}
						await this.#writeFile(target, plaintext_suffix, 0, plaintext_suffix.length, write_position);
						break;
					}
					position += 992n;
				}
				continue;
			}
			if (filetype !== 0x4000) {	// Not a directory. Treat it as a normal file
				if (this.#loaded_inode !== loaded_inode) {
					this.#loaded_inode = loaded_inode;
					await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
				}
				const filesize = this.#getFileSize();
				const extents = !!(flags & 0x80000);
				for (const block of range(0n, (filesize + this.#blocksize - 1n) / this.#blocksize)) {
					const mapped = await this.#getFileBlock(block, extents);
					// Don't blindly trust the file size
					// The system can under-allocate blocks for sparse files
					// It can also skip blocks in the middle of a file
					if (mapped !== false) {
						target.setBlockOptions(mapped, derived_key, block);
					}
				}
				continue;
			}
			let position = (flags & 0x10000000)?4n:0n;
			while (true) {
				if (this.#loaded_inode !== loaded_inode) {
					this.#loaded_inode = loaded_inode;
					await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
				}
				if (await this.#readFile(temp_buffer, 0, 8, position) < 8) {
					break;
				}
				if (this.#loaded_inode !== loaded_inode) {
					this.#loaded_inode = loaded_inode;
					await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
				}
				const next = temp_buffer.readUInt16LE(0x4);
				const name_len = this.#direntry_with_filetype?temp_buffer.readUInt8(0x6):temp_buffer.readUInt16LE(0x6);
				if (next < 8 + name_len) {
					break;
				}
				if (name_len > 15) {
					const name_buffer = Buffer.alloc(name_len);
					await this.#readFile(name_buffer, 0, name_len, position + 8n);
					if (this.#loaded_inode !== loaded_inode) {
						this.#loaded_inode = loaded_inode;
						await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
					}
					const decrypted_name = decryptCts(name_buffer, derived_key.subarray(0, 32));
					await this.#writeFile(target, decrypted_name, 0, name_len, position + 8n);
					let new_len = name_len;
					while (new_len > 0 && decrypted_name[new_len - 1] === 0) {
						new_len--;
					}
					if (new_len < name_len) {
						if (this.#loaded_inode !== loaded_inode) {
							this.#loaded_inode = loaded_inode;
							await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
						}
						if (this.#direntry_with_filetype) {
							temp_buffer.writeUInt8(new_len);
							await this.#writeFile(target, temp_buffer, 0, 1, position + 6n);
						}
						else {
							temp_buffer.writeUInt16(new_len);
							await this.#writeFile(target, temp_buffer, 0, 2, position + 6n);
						}
					}
				}
				position += BigInt(next);
			}
		}
		yield [this.#inodes_count, this.#inodes_count];
	}

	async #loadGroupDescriptor(index) {
		index = BigInt(index);
		let meta_bg_group = 0n;
		if (this.#meta_bg && index >= this.#first_meta_bg * this.#blocks_per_group) {
			const meta_bg_group_count = this.#blocks_per_group * (this.#blocksize / BigInt(this.#group_descriptor.length));
			index -= this.#first_meta_bg * this.#blocks_per_group;
			const meta_bg_index = index / meta_bg_group_count;
			index %= meta_bg_group_count;
			meta_bg_group = this.#first_meta_bg + meta_bg_index * meta_bg_group_count;
		}
		let start_offset;
		if (meta_bg_group === 0n) {
			if (this.#blocksize < 2048n) {
				start_offset = this.#blocksize << 1n;
			}
			else {
				start_offset = this.#blocksize;
			}
		}
		else {
			start_offset = this.#blocksize * (meta_bg_group * this.#blocks_per_group + 1n);
		}
		const group_start = start_offset + index * BigInt(this.#group_descriptor.length);
		if (this.#loaded_group_descriptor !== group_start) {
			this.#loaded_group_descriptor = group_start;
			await this.#file.read(this.#group_descriptor, 0, this.#group_descriptor.length, Number(group_start));
		}
	}

	async #loadInode(index) {
		index = BigInt(index);
		if (index < 1n) {
			return false;
		}
		const group_index = ((index - 1n) / this.#inodes_per_group) << this.#flex_bg;
		const inode_index = (index - 1n) % this.#inodes_per_group;
		await this.#loadGroupDescriptor(group_index);
		if (this.#group_descriptor.readUInt32LE(0x12) & 5 !== 4) {
			return false;
		}
		const inode_bitmap_lo = this.#group_descriptor.readUInt32LE(0x4);
		const inode_bitmap_hi = (this.#group_descriptor.length < 0x28)?0:this.#group_descriptor.readUInt32LE(0x24);
		const inode_bitmap_start = (BigInt(inode_bitmap_lo) + (BigInt(inode_bitmap_hi) << 32n)) * this.#blocksize + (inode_index >> 3n);
		if (this.#loaded_inode_bitmap !== inode_bitmap_start) {
			this.#loaded_inode_bitmap = inode_bitmap_start;
			await this.#file.read(this.#inode_bitmap, 0, 1, Number(inode_bitmap_start));
		}
		if (!(this.#inode_bitmap[0] & (1 << Number(inode_index & 7n)))) {
			return false;
		}
		const inode_table_lo = this.#group_descriptor.readUInt32LE(0x8);
		const inode_table_hi = (this.#group_descriptor.length < 0x2C)?0:this.#group_descriptor.readUInt32LE(0x28);
		const inode_start = (BigInt(inode_table_lo) + (BigInt(inode_table_hi) << 32n)) * this.#blocksize + inode_index * BigInt(this.#inode.length);
		if (this.#loaded_inode !== inode_start) {
			this.#loaded_inode = inode_start;
			await this.#file.read(this.#inode, 0, this.#inode.length, Number(inode_start));
		}
		return true;
	}

	async #getFileBlock(block, extents) {
		if (extents) {
			if (this.#inode.readUInt16LE(0x28) !== 0xF30A) {
				throw new Error('Invalid extents magic signature');
			}
			let extent_block;
			if (this.#last_extent_inode === this.#loaded_inode && block >= this.#last_extent_min && (this.#last_extent_max <= 0n || block < this.#last_extent_max)) {
				extent_block = this.#last_extent_block;
			}
			else {
				const entries = this.#inode.readUInt16LE(0x2A);
				const depth = this.#inode.readUInt16LE(0x2E);
				const next = binarySearch(entries, (i) => BigInt(this.#inode.readUInt32LE(0x34 + 12 * i)) > block);
				if (next < 1) {
					return false;
				}
				if (depth === 0) {
					const shift = block - BigInt(this.#inode.readUInt32LE(0x28 + 12 * next));
					let len = BigInt(this.#inode.readUInt32LE(0x2C + 12 * next));
					if (len > 0x8000n) {
						len -= 0x8000n;
					}
					if (shift >= len) {
						return false;
					}
					return BigInt(this.#inode.readUInt32LE(0x30 + 12 * next)) + (BigInt(this.#inode.readUInt16LE(0x2E + 12 * next)) << 32n) + shift;
				}
				extent_block = (BigInt(this.#inode.readUInt32LE(0x2C + 12 * next)) + (BigInt(this.#inode.readUInt16LE(0x30 + 12 * next)) << 32n)) * this.#blocksize;
				this.#last_extent_inode = this.#loaded_inode;
				this.#last_extent_block = extent_block;
				this.#last_extent_min = BigInt(this.#inode.readUInt32LE(0x28 + 12 * next));
				this.#last_extent_max = (next < entries)?BigInt(this.#inode.readUInt32LE(0x34 + 12 * next)):0n;
			}
			while (true) {
				await this.#file.read(this.#temp_buffer, 0, 8, Number(extent_block));
				if (this.#temp_buffer.readUInt16LE(0x0) !== 0xF30A) {
					throw new Error('Invalid extents magic signature');
				}
				const entries = this.#temp_buffer.readUInt16LE(0x2);
				const depth = this.#temp_buffer.readUInt16LE(0x6);
				const next = await binarySearchAsync(entries, async (i) => {
					await this.#file.read(this.#temp_buffer, 0, 4, Number(extent_block + 12n * BigInt(i) + 12n));
					return BigInt(this.#temp_buffer.readUInt32LE(0)) > block;
				});
				if (next < 1) {
					return false;
				}
				await this.#file.read(this.#temp_buffer, 0, 4, Number(extent_block + 12n * BigInt(next)));
				const min = BigInt(this.#temp_buffer.readUInt32LE(0));
				let max = 0n;
				if (next < entries) {
					await this.#file.read(this.#temp_buffer, 0, 4, Number(extent_block + 12n * BigInt(next) + 12n));
					max = BigInt(this.#temp_buffer.readUInt32LE(0));
				}
				await this.#file.read(this.#temp_buffer, 0, 8, Number(extent_block + 12n * BigInt(next) + 4n));
				if (depth === 0) {
					const shift = block - min;
					let len = BigInt(this.#temp_buffer.readUInt16LE(0));
					if (len > 0x8000n) {
						len -= 0x8000n;
					}
					if (shift >= len) {
						return false;
					}
					return BigInt(this.#temp_buffer.readUInt32LE(0x4)) + (BigInt(this.#temp_buffer.readUInt16LE(0x2)) << 32n) + shift;
				}
				extent_block = (BigInt(this.#temp_buffer.readUInt32LE(0x0)) + (BigInt(this.#temp_buffer.readUInt16LE(0x4)) << 32n)) * this.#blocksize;
				this.#last_extent_block = extent_block;
				this.#last_extent_min = min;
				this.#last_extent_max = max;
			}
		}
		if (block < 12n) {
			const indirect0 = BigInt(this.#inode.readUInt32LE(0x28 + Number(block)));
			if (!indirect0) {
				return false;
			}
			return indirect0;
		}
		const indirects_per_block = this.#blocksize >> 2n;
		block -= 12n;
		if (block < indirects_per_block) {
			const indirect0 = BigInt(this.#inode.readUInt32LE(0x58));
			if (!indirect0) {
				return false;
			}
			const indirect1 = indirect0 * this.#blocksize + (block << 2n);
			if (this.#loaded_indirects[0] !== indirect1) {
				this.#loaded_indirects[0] = indirect1;
				await this.#file.read(this.#temp_buffer, 0, 4, Number(indirect1));
				this.#indirects[0] = BigInt(this.#temp_buffer.readUInt32LE(0));
			}
			if (!this.#indirects[0]) {
				return false;
			}
			return this.#indirects[0] || false;
		}
		block -= indirects_per_block;
		const indirects_per_block2 = indirects_per_block*indirects_per_block;
		if (block < indirects_per_block2) {
			const indirect0 = BigInt(this.#inode.readUInt32LE(0x5C));
			if (!indirect0) {
				return false;
			}
			const indirect1 = indirect0 * this.#blocksize + ((block / indirects_per_block) << 2n);
			block %= indirects_per_block;
			if (this.#loaded_indirects[0] !== indirect1) {
				this.#loaded_indirects[0] = indirect1;
				await this.#file.read(this.#temp_buffer, 0, 4, Number(indirect1));
				this.#indirects[0] = BigInt(this.#temp_buffer.readUInt32LE(0));
			}
			if (!this.#indirects[0]) {
				return false;
			}
			const indirect2 = this.#indirects[0] * this.#blocksize + (block << 2n);
			if (this.#loaded_indirects[1] !== indirect2) {
				this.#loaded_indirects[1] = indirect2;
				await this.#file.read(this.#temp_buffer, 0, 4, Number(indirect2));
				this.#indirects[1] = BigInt(this.#temp_buffer.readUInt32LE(0));
			}
			if (!this.#indirects[1]) {
				return false;
			}
			return this.#indirects[1];
		}
		block -= indirects_per_block2;
		const indirect0 = BigInt(this.#inode.readUInt32LE(0x60));
		if (!indirect0) {
			return false;
		}
		const indirect1 = indirect0 * this.#blocksize + ((block / indirects_per_block2) << 2n);
		block %= indirects_per_block2;
		if (this.#loaded_indirects[0] !== indirect1) {
			this.#loaded_indirects[0] = indirect1;
			await this.#file.read(this.#temp_buffer, 0, 4, Number(indirect1));
			this.#indirects[0] = BigInt(this.#temp_buffer.readUInt32LE(0));
		}
		if (!this.#indirects[0]) {
			return false;
		}
		const indirect2 = this.#indirects[0] * this.#blocksize + ((block / indirects_per_block) << 2n);
		block %= indirects_per_block;
		if (this.#loaded_indirects[1] !== indirect2) {
			this.#loaded_indirects[1] = indirect2;
			await this.#file.read(this.#temp_buffer, 0, 4, Number(indirect2));
			this.#indirects[1] = BigInt(this.#temp_buffer.readUInt32LE(0));
		}
		if (!this.#indirects[1]) {
			return false;
		}
		const indirect3 = this.#indirects[1] * this.#blocksize + (block << 2n);
		if (this.#loaded_indirects[2] !== indirect3) {
			this.#loaded_indirects[2] = indirect3;
			await this.#file.read(this.#temp_buffer, 0, 4, Number(indirect3));
			this.#indirects[2] = BigInt(this.#temp_buffer.readUInt32LE(0));
		}
		if (!this.#indirects[2]) {
			return false;
		}
		return this.#indirects[2];
	}

	#getFileSize() {
		return BigInt(this.#inode.readUInt32LE(0x4)) + (BigInt(this.#inode.readUInt32LE(0x6C)) << 32n);
	}

	async #readFromBlock(buffer, offset, length, block, extents, block_offset, key) {
		const mapped = await this.#getFileBlock(block, extents);
		if (mapped === false) {
			buffer.fill(0, offset, length);
			return;
		}
		const res = await this.#file.read(buffer, offset, length, Number(BigInt(mapped) * this.#blocksize + block_offset), ...key?[key, block]:[]);
		if (res < length) {
			buffer.fill(0, offset + res, length - res);
		}
	}

	async #readFile(buffer, offset, length, position, key) {
		let ret = 0;
		position = BigInt(position);
		if (length <= 0n) {
			return 0;
		}
		if (position + BigInt(length) <= 0n) {
			return 0;
		}
		if (position < 0n) {
			buffer.fill(0, offset, Number(position));
			ret += Number(position);
			offset += Number(position);
			length -= Number(position);
			position = 0n;
		}
		const filesize = this.#getFileSize();
		if (position >= filesize) {
			return 0;
		}
		if (position + BigInt(length) > filesize) {
			length = Number(filesize - position);
		}
		const flags = this.#inode.readUInt32LE(0x20);
		if (flags & 0x10000000) {
			if (position < 60n) {
				const srcoffset = 0x28 + Number(position);
				if (position + BigInt(length) <= 60n) {
					this.#inode.copy(buffer, offset, srcoffset, srcoffset + length);
					return ret + length;
				}
				this.#inode.copy(buffer, offset, srcoffset, 0x64);
				ret += 0x64 - srcoffset;
				offset += 0x64 - srcoffset;
				length -= 0x64 - srcoffset;
				position = 60n;
			}
			if (length > 0) {
				ret += await this.#readExtendedAttribute(7, Buffer.from('data'), buffer, offset, length, position - 60n);
			}
			return ret;
		}
		const extents = !!(flags & 0x80000);
		let block = position / this.#blocksize;
		const start_offset = position % this.#blocksize;
		if (start_offset + BigInt(length) <= this.#blocksize) {
			await this.#readFromBlock(buffer, offset, length, block, extents, start_offset, key);
			return ret + length;
		}
		await this.#readFromBlock(buffer, offset, Number(this.#blocksize - start_offset), block, extents, start_offset, key);
		ret += Number(this.#blocksize - start_offset);
		offset += Number(this.#blocksize - start_offset);
		length -= Number(this.#blocksize - start_offset);
		position += this.#blocksize - start_offset;
		block++;
		while (BigInt(length) > this.#blocksize) {
			await this.#readFromBlock(buffer, offset, Number(this.#blocksize), block, extents, 0, key);
			ret += Number(this.#blocksize);
			offset += Number(this.#blocksize);
			length -= Number(this.#blocksize);
			position += Number(this.#blocksize);
			block++;
		}
		await this.#readFromBlock(buffer, offset, length, block, extents, 0n, key);
		return ret + length;
	}

	async #writeFile(target, buffer, offset, length, position) {
		let ret = 0;
		position = BigInt(position);
		if (position + BigInt(length) < 0n) {
			return 0;
		}
		if (position < 0n) {
			ret += Number(position);
			offset += Number(position);
			length -= Number(position);
			position = 0n;
		}
		const filesize = this.#getFileSize();
		if (position >= filesize) {
			return 0;
		}
		if (position + BigInt(length) > filesize) {
			length = Number(filesize - position);
		}
		const flags = this.#inode.readUInt32LE(0x20);
		if (flags & 0x10000000) {
			if (position < 60n) {
				const srcoffset = 0x28 + Number(position);
				if (position + BigInt(length) <= 60n) {
					target.write(buffer, offset, length, this.#loaded_inode + BigInt(srcoffset));
					return ret + length;
				}
				target.write(buffer, offset, 0x64 - srcoffset, this.#loaded_inode + BigInt(srcoffset));
				ret += 0x64 - srcoffset;
				offset += 0x64 - srcoffset;
				length -= 0x64 - srcoffset;
				position = 60n;
			}
			if (length > 0) {
				ret += await this.#writeExtendedAttribute(target, 7, Buffer.from('data'), buffer, offset, length, position - 60n);
			}
			return ret;
		}
		const extents = !!(flags & 0x80000);
		let block = position / this.#blocksize;
		const start_offset = position % this.#blocksize;
		const start_block = await this.#getFileBlock(block, extents);
		if (start_block === false) {
			// Cannot write into unallocated/lazy block
			return ret;
		}
		const start_position = start_block * this.#blocksize + start_offset;
		if (start_offset + BigInt(length) <= this.#blocksize) {
			target.write(buffer, offset, length, start_position);
			return ret + length;
		}
		target.write(buffer, offset, Number(this.#blocksize - start_offset), start_position);
		ret += Number(this.#blocksize - start_offset);
		offset += Number(this.#blocksize - start_offset);
		length -= Number(this.#blocksize - start_offset);
		position += this.#blocksize - start_offset;
		block++;
		while (BigInt(length) > this.#blocksize) {
			const mid_block = await this.#getFileBlock(block, extents);
			if (mid_block === false) {
				// Cannot write into unallocated/lazy block
				return ret;
			}
			target.write(buffer, offset, Number(this.#blocksize), mid_block * this.#blocksize);
			ret += Number(this.#blocksize);
			offset += Number(this.#blocksize);
			length -= Number(this.#blocksize);
			position += Number(this.#blocksize);
			block++;
		}
		const end_block = await this.#getFileBlock(block, extents);
		if (end_block === false) {
			// Cannot write into unallocated/lazy block
			return ret;
		}
		target.write(buffer, offset, length, end_block * this.#blocksize);
		return ret + length;
	}

	async *#iterateAttributes() {
		if (this.#inode.length > 0x82) {
			const extra_size = this.#inode.readUInt16LE(0x80);
			if (this.#inode.length >= 0x94 + extra_size && this.#inode.readUInt32LE(0x80 + extra_size) === 0xEA020000) {
				const attribute_root = 0x84 + extra_size;
				let attribute = attribute_root;
				while (attribute + 0x10 < this.#inode.length) {
					const name_len = this.#inode.readUInt8(attribute);
					const name_index = this.#inode.readUInt8(attribute + 1);
					if (name_len === 0 && name_index === 0) {
						break;
					}
					if (attribute + 0x10 + name_len >= this.#inode.length) {
						break;
					}
					const value_size = this.#inode.readUInt32LE(attribute + 0x8);
					const attribute_offset = BigInt(this.#inode.readUInt16LE(attribute + 0x2));
					const attribute_inode = this.#ea_inode?this.#inode.readUInt32LE(attribute + 0x4):0;
					yield {
						index: name_index,
						name: this.#inode.subarray(attribute + 0x10, attribute + 0x10 + name_len),
						root: this.#loaded_inode + BigInt(attribute),
						size: value_size,
						read: async (buffer, offset, length, position) => {
							position = BigInt(position);
							if (position >= BigInt(value_size)) {
								return 0;
							}
							if (position + BigInt(length) > BigInt(value_size)) {
								length = value_size - Number(position);
							}
							position += attribute_offset;
							if (attribute_inode !== 0) {
								const loaded_inode = this.#loaded_inode;
								if (!await this.#loadInode(attribute_inode)) {
									return 0;
								}
								const ret = await this.#readFile(buffer, offset, length, position);
								if (this.#loaded_inode !== loaded_inode) {
									this.#loaded_inode = loaded_inode;
									await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
								}
								return ret;
							}
							position += BigInt(attribute_root);
							if (position + BigInt(length) <= BigInt(this.#inode.length)) {
								this.#inode.copy(buffer, offset, Number(position), Number(position) + length);
								return length;
							}
							let ret = 0;
							if (position < BigInt(this.#inode.length)) {
								this.#inode.copy(buffer, offset, Number(position), this.#inode.length);
								ret = this.#inode.length - Number(position);
								offset += ret;
								length -= ret;
								position = BigInt(this.#inode.length);
							}
							position += this.#loaded_inode;
							return ret + await this.#file.read(buffer, offset, length, Number(position));
						},
						write: async (target, buffer, offset, length, position) => {
							position = BigInt(position);
							if (position >= BigInt(value_size)) {
								return 0;
							}
							if (position + BigInt(length) > BigInt(value_size)) {
								length = value_size - Number(position);
							}
							position += attribute_offset;
							if (attribute_inode !== 0) {
								const loaded_inode = this.#loaded_inode;
								if (!await this.#loadInode(attribute_inode)) {
									return 0;
								}
								const ret = target.write(buffer, offset, length, position);
								if (this.#loaded_inode !== loaded_inode) {
									this.#loaded_inode = loaded_inode;
									await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
								}
								return ret;
							}
							position += BigInt(attribute_root);
							if (position + BigInt(length) <= BigInt(this.#inode.length)) {
								target.write(buffer, offset, length, this.#loaded_inode + position);
								return length;
							}
							let ret = 0;
							if (position < BigInt(this.#inode.length)) {
								target.write(buffer, offset, this.#inode.length - Number(position), this.#loaded_inode + position);
								ret = this.#inode.length - Number(position);
								offset += ret;
								length -= ret;
								position = BigInt(this.#inode.length);
							}
							position += this.#loaded_inode;
							return ret + target.write(buffer, offset, length, position);
						}
					};
					attribute += (0x10 + name_len + 3) & ~3;
				}
			}
		}
		let name_buffer = Buffer.alloc(0x10);
		const attribute_root = (BigInt(this.#inode.readUInt32LE(0x68)) + (BigInt(this.#inode.readUInt16LE(0x76)) << 32n)) * this.#blocksize;
		if (attribute_root === 0n || await this.#file.read(name_buffer, 0, 4, Number(attribute_root)) < 4) {
			return 0;
		}
		if (name_buffer.readUInt32LE(0) !== 0xEA020000) {
			return 0;
		}
		let attribute = attribute_root + 0x20n;
		while (true) {
			if (await this.#file.read(name_buffer, 0, 0x10, Number(attribute)) < 0x10) {
				break;
			}
			const name_len = name_buffer.readUInt8(0);
			const name_index = name_buffer.readUInt8(1);
			if (name_len === 0 && name_index === 0) {
				break;
			}
			const value_size = name_buffer.readUInt32LE(0x8);
			const attribute_offset = BigInt(name_buffer.readUInt16LE(0x2));
			const attribute_inode = this.#ea_inode?name_buffer.readUInt32LE(0x4):0;
			if (name_buffer.length < name_len) {
				name_buffer = Buffer.alloc(name_len);
			}
			if (await this.#file.read(name_buffer, 0, name_len, Number(attribute + 0x10n)) < name_len) {
				break;
			}
			yield {
				index: name_index,
				name: name_buffer.subarray(0, name_len),
				root: attribute,
				size: value_size,
				read: async (buffer, offset, length, position) => {
					position = BigInt(position);
					if (position >= BigInt(value_size)) {
						return 0;
					}
					if (position + BigInt(length) > BigInt(value_size)) {
						length = value_size - Number(position);
					}
					position += attribute_offset;
					if (attribute_inode !== 0) {
						if (!await this.#loadInode(attribute_inode)) {
							return 0;
						}
						return await this.#readFile(buffer, offset, length, position);
					}
					position += attribute_root;
					return await this.#file.read(buffer, offset, length, Number(position));
				},
				write: async (target, buffer, offset, length, position) => {
					position = BigInt(position);
					if (position >= BigInt(value_size)) {
						return 0;
					}
					if (position + BigInt(length) > BigInt(value_size)) {
						length = value_size - Number(position);
					}
					position += attribute_offset;
					if (attribute_inode !== 0) {
						if (!await this.#loadInode(attribute_inode)) {
							return 0;
						}
						return target.write(buffer, offset, length, position);
					}
					position += attribute_root;
					return target.write(buffer, offset, length, position);
				}
			};
			attribute += (0x10n + BigInt(name_len) + 3n) & ~3n;
		}
	}

	async #readExtendedAttribute(index, name, buffer, offset, length, position) {
		for await (const attribute of this.#iterateAttributes()) {
			if (attribute.index !== index || !name.equals(attribute.name)) {
				continue;
			}
			return await attribute.read(buffer, offset, length, Number(position));
		}
		return 0;
	}

	async #writeExtendedAttribute(target, index, name, buffer, offset, length, position) {
		for await (const attribute of this.#iterateAttributes()) {
			if (attribute.index !== index || !name.equals(attribute.name)) {
				continue;
			}
			return await attribute.write(target, buffer, offset, length, position);
		}
		return 0;
	}

	async #createDirent(name) {
		const loaded_inode = this.#loaded_inode;
		const mode = this.#inode.readUInt16LE(0);
		const flags = this.#inode.readUInt32LE(0x20);
		const stats = {
			size: this.#getFileSize()
		};
		let derived_key = null;
		if (flags & 0x800) {
			const profile = Buffer.alloc(40);
			const res = await this.#readExtendedAttribute(9, Buffer.from('c'), profile, 0, profile.length, 0n)
			// Assume v2 profile (40 bytes, first byte 02, HKDF-SHA512 key derivation)
			// and not v1 profile (28 bytes, first byte 01, AES-128-ECB key derivation)
			// To implement v1 profile, it would also be necessary to implement v1
			// key identification, which _should_ be double SHA512 truncated to 8 bytes.
			// However, testing this would require a v1 encrypted partition
			if (res >= 40) {
				const masterkey = this.#keys.get(profile.subarray(8, 24).toString('hex'));
				if (masterkey) {
					// A full HKDF-SHA512 implementation can derive a key of any length
					// But we'll never use more than 64 bytes, so one round is enough
					const hmac1 = createHmac('sha512', '');
					hmac1.update(masterkey);
					const hmac2 = createHmac('sha512', hmac1.digest());
					hmac2.update('fscrypt\0');
					hmac2.update(Buffer.from([2]));	// Context=Per-file Encryption Key
					hmac2.update(profile.subarray(24));
					hmac2.update(Buffer.from([1]));	// Counter=1
					derived_key = hmac2.digest();
				}
			}
		}
		return {
			isBlockDevice: () => {
				return (mode & 0xF000) === 0x6000;
			},
			isCharacterDevice: () => {
				return (mode & 0xF000) === 0x2000;
			},
			isDirectory: () => {
				return (mode & 0xF000) === 0x4000;
			},
			isFIFO: () => {
				return (mode & 0xF000) === 0x1000;
			},
			isFile: () => {
				return (mode & 0xF000) === 0x8000;
			},
			isSocket: () => {
				return (mode & 0xF000) === 0xC000;
			},
			isSymbolicLink: () => {
				return (mode & 0xF000) === 0xA000;
			},
			name: name,
			open: () => {
				if ((mode & 0xF000) === 0x4000) {
					const temp_buffer = Buffer.alloc(8);
					let position = (flags & 0x10000000)?4n:0n;
					const ret = {
						read: async () => {
							while (true) {
								if (this.#loaded_inode !== loaded_inode) {
									this.#loaded_inode = loaded_inode;
									await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
								}
								if (await this.#readFile(temp_buffer, 0, 8, position) < 8) {
									return null;
								}
								if (this.#loaded_inode !== loaded_inode) {
									this.#loaded_inode = loaded_inode;
									await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
								}
								const entry_inode = temp_buffer.readUInt32LE(0);
								const next = temp_buffer.readUInt16LE(0x4);
								const name_len = this.#direntry_with_filetype?temp_buffer.readUInt8(0x6):temp_buffer.readUInt16LE(0x6);
								if (next < 8 + name_len) {
									return null;
								}
								const name_buffer = Buffer.alloc(name_len);
								await this.#readFile(name_buffer, 0, name_len, position + 8n);
								position += BigInt(next);
								if (await this.#loadInode(entry_inode)) {
									return await this.#createDirent(derived_key && name_buffer.length > 15?decryptCts(name_buffer, derived_key.subarray(0, 32)).toString().replace(/\x00+$/, ''):name_buffer.toString());
								}
							}
						}
					};
					ret[Symbol.asyncIterator] = async function*() {
						let dirent;
						while ((dirent = await ret.read()) !== null) {
							yield dirent;
						}
					};
					return ret;
				}
				return {
					read: async (buffer, offset, length, position) => {
						if (this.#loaded_inode !== loaded_inode) {
							this.#loaded_inode = loaded_inode;
							await this.#file.read(this.#inode, 0, this.#inode.length, Number(loaded_inode));
						}
						// FIXME: Symlinks need special handling that is closer to directories than to files
						return await this.#readFile(buffer, offset, length, position, derived_key);
					},
					stat: async () => {
						return stats;
					}
				};
			}
		};
	}
}

class SQLiteDatabase {
	#dev;
	#u;
	#x_table;
	#x_index;
	#m;
	#encoding;
	#encoding_encoder;
	#encoding_decoder;

	static async open(file) {
		const self = new SQLiteDatabase();
		const header = Buffer.alloc(100);
		if (await file.read(header, 0, 100, 0) < 100) {
			throw new Error('Incomplete SQLite header');
		}
		if (!header.subarray(0, 16).equals(Buffer.from('SQLite format 3\0'))) {
			throw new Error('Not a valid SQLite database file');
		}
		const blocksize = header.readUInt16BE(16);
		if (blocksize !== 1 && blocksize < 512) {
			throw new Error('Page size too small: ' + blocksize);
		}
		self.#dev = await BlockDevFile.open(file, blocksize === 1?65536:blocksize);
		self.#u = BigInt(self.#dev.blockSize - header.readUInt8(20));
		self.#x_table = self.#u - 35n;
		self.#x_index = ((self.#u - 12n) * 64n / 255n) - 23n;
		self.#m = ((self.#u - 12n) * 32n / 255n) - 23n;
		switch (header.readUInt32BE(56)) {
			case 0:	// Legacy file. Use utf8 as default
			case 1:
				self.#encoding = 'utf8';
				self.#encoding_encoder = (str) => Buffer.from(str);
				self.#encoding_decoder = (buf) => buf.toString();
				break;
			case 2:
				self.#encoding = 'utf16le';
				self.#encoding_encoder = (str) => Buffer.from(str, 'utf16le');
				self.#encoding_decoder = (buf) => buf.toString('utf16le');
				break;
			case 3:
				self.#encoding = 'utf16be';
				self.#encoding_encoder = (str) => Buffer.from(str, 'utf16le').swap16();
				self.#encoding_decoder = (buf) => buf.swap16().toString('utf16le');
				break;
			default:
				throw new Error('Unknown encoding: ' + header.readUInt32BE(56));
		}
		return self;
	}

	async *getTableRows(name) {
		const type_buffer = this.#encoding_encoder('table');
		const name_buffer = this.#encoding_encoder(name);
		const buffer = Buffer.alloc((name_buffer.length > type_buffer.length?name_buffer.length:type_buffer) + 1);
		let root = false;
		tables: for await (const [buffer_iterator, total_size] of this.#iterateBTree(0n)) {
			const [type, name, _, rootpage] = await iteratorToArrayAsync(this.#iterateRecords(buffer_iterator, total_size), 4);
			for await (const str of using(await type.open())) {
				if (await str.read(buffer, 0, type_buffer.length + 1, 0) !== type_buffer.length) {
					continue tables;
				}
				if (!buffer.subarray(0, type_buffer.length).equals(type_buffer)) {
					continue tables;
				}
			}
			for await (const str of using(await name.open())) {
				if (await str.read(buffer, 0, name_buffer.length + 1, 0) !== name_buffer.length) {
					continue tables;
				}
				if (!buffer.subarray(0, name_buffer.length).equals(name_buffer)) {
					continue tables;
				}
			}
			root = rootpage.value - 1n;
			break;
		}
		if (root !== false) {
			for await (const [buffer_iterator, total_size] of this.#iterateBTree(root)) {
				yield this.#iterateRecords.bind(this, buffer_iterator, total_size);
			}
		}
	}

	async *#iterateBTree(block) {
		const buffer = await this.#dev.readBlock(block);
		const blockstart = block?0:100;
		const blocktype = buffer.readUInt8(blockstart);
		const cellcount = buffer.readUInt16BE(blockstart + 3);
		const cellarray = blockstart + ((blocktype === 2 || blocktype === 5)?12:8);
		for (const index of range(0, cellcount)) {
			const sequence_reader = new FileSequenceReader(new FileHandleBuffer(buffer), buffer.readUInt16BE(cellarray + (index << 1)));
			if (blocktype === 2 || blocktype === 5) {
				yield *this.#iterateBTree(await sequence_reader.readUInt32BE() - 1);
				continue;
			}
			const payload_size = await this.#readVarint(sequence_reader);
			if (blocktype === 13) {
				// Skip row id. We don't use it
				await this.#readVarint(sequence_reader);
			}
			const x = (blocktype === 10)?this.#x_table:this.#x_index;
			const k = this.#m + ((payload_size - this.#m) % (this.#u - 4n));
			const payload_start = sequence_reader.position;
			const payload_end = (payload_size > x)?(k > x)?(payload_start + this.#m):(payload_start + k):(payload_start + payload_size);
			const overflow = (payload_size > x)?buffer.readUInt32BE(Number(payload_end)):0;
			const dev = this.#dev;
			function rest(block, remaining) {
				return async function *() {
					let remaining_size = remaining;
					let next_block = block;
					let block_buffer = await dev.readBlock(next_block);
					while (remaining_size + 4n > BigInt(block_buffer.length)) {
						remaining_size -= BigInt(block_buffer.length) - 4n;
						next_block = block_buffer.readUInt32BE(0);
						if (yield block_buffer.subarray(4)) {
							while (yield rest(next_block, remaining_size));
						}
						block_buffer = await dev.readBlock(next_block);
					}
					if (yield block_buffer.subarray(4, Number(remaining_size) + 4)) {
						while (yield async function *() {});
					}
				};
			}
			yield [async function *() {
				if (yield buffer.subarray(Number(payload_start), Number(payload_end))) {
					if (payload_size > x) {
						while (yield rest(overflow, payload_start - payload_end + payload_size));
					}
					else {
						while (yield async function *() {});
					}
				}
				if (payload_size > x) {
					yield *rest(overflow, payload_start - payload_end + payload_size);
				}
			}, payload_size];
		}
	}

	async *#iterateRecords(buffer_iterator, total_size) {
		for await (const header of using(await FileHandleBufferIterator.open(buffer_iterator, total_size))) {
			const header_reader = new FileSequenceReader(header);
			const header_size = await this.#readVarint(header_reader);
			header_reader.limit = header_size;
			for await (const data of using((await header.snapshotAt(header_size)).open(buffer_iterator))) {
				const data_reader = new FileSequenceReader(data);
				while (!await header_reader.isEOF()) {
					const type = await this.#readVarint(header_reader);
					switch (type) {
						case 0n:
							yield {
								type: 0,
								value: null
							};
							break;
						case 1n:
							yield {
								type: 1,
								value: BigInt(await data_reader.readInt8())
							};
							break;
						case 2n:
							yield {
								type: 1,
								value: BigInt(await data_reader.readInt16BE())
							};
							break;
						case 3n:
							const high8bits = BigInt(await data_reader.readInt8()) << 16n;
							yield {
								type: 1,
								value: high8bits | BigInt(await data_reader.readUInt16BE())
							};
							break;
						case 4n:
							yield {
								type: 1,
								value: BigInt(await data_reader.readInt32BE())
							};
							break;
						case 5n:
							const high16bits = BigInt(await data_reader.readInt16BE()) << 32n;
							yield {
								type: 1,
								value: high16bits | BigInt(await data_reader.readUInt32BE())
							};
							break;
						case 6n:
							yield {
								type: 1,
								value: BigInt(await data_reader.readBigInt64BE())
							};
							break;
						case 7n:
							yield {
								type: 1,
								value: await data_reader.readDoubleBE()
							};
							break;
						case 8n:
							yield {
								type: 1,
								value: 0n
							};
							break;
						case 9n:
							yield {
								type: 1,
								value: 1n
							};
							break;
						default:
							if (type < 12n) {
								throw new Error('Invalid type: ' + type);
							}
							const length = (type >> 1n) - 6n;
							const position = data_reader.position;
							data_reader.skip(length);
							if (type & 1n) {
								yield {
									type: 3,
									open: (await data.snapshotAt(position, position + length)).open,
									encoding: this.#encoding,
									encoding_encoder: this.#encoding_encoder,
									encoding_decoder: this.#encoding_decoder
								};
							}
							else {
								yield {
									type: 2,
									open: (await data.snapshotAt(position, position + length)).open
								};
							}
					}
				}
			}
		}
	}

	async #readVarint(sequence_reader) {
		let ret = 0n;
		for (const index of range(0, 8)) {
			const val = await sequence_reader.readUInt8();
			ret <<= 7n;
			ret |= BigInt(val & 0x7F);
			if (!(val & 0x80)) {
				return ret;
			}
		}
		const val = await sequence_reader.readUInt8();
		ret <<= 8n;
		ret |= BigInt(val);
		return ret;
	}
}

class ChangeLogger {
	#block_options;
	#blocks;
	#blocksize;
	#segment_count;

	constructor(blocksize) {
		this.#block_options = new Map();
		this.#blocks = new Map();
		this.#blocksize = blocksize;
		this.#segment_count = 0;
	}

	write(buffer, offset, length, position) {
		position = BigInt(position)
		let block = position / BigInt(this.#blocksize)
		const start_offset = Number(position % BigInt(this.#blocksize))
		if (start_offset + length <= this.#blocksize) {
			this.#writeToBlock(block, start_offset, buffer, offset, length);
			return;
		}
		this.#writeToBlock(block, start_offset, buffer, offset, this.#blocksize - start_offset);
		offset += this.#blocksize - start_offset;
		length -= this.#blocksize - start_offset;
		block++;
		while (length > this.#blocksize) {
			this.#writeToBlock(block, 0, buffer, offset, this.#blocksize);
			offset += this.#blocksize;
			length -= this.#blocksize;
			block++;
		}
		this.#writeToBlock(block, 0, buffer, offset, length);
	}

	apply(buffer, offset, length, position) {
		position = BigInt(position)
		let block = position / BigInt(this.#blocksize)
		const start_offset = Number(position % BigInt(this.#blocksize))
		if (start_offset + length <= this.#blocksize) {
			this.#applyBlock(block, start_offset, buffer, offset, length);
			return;
		}
		this.#applyBlock(block, start_offset, buffer, offset, this.#blocksize - start_offset);
		offset += this.#blocksize - start_offset;
		length -= this.#blocksize - start_offset;
		block++;
		while (length > this.#blocksize) {
			this.#applyBlock(block, 0, buffer, offset, this.#blocksize);
			offset += this.#blocksize;
			length -= this.#blocksize;
			block++;
		}
		this.#applyBlock(block, 0, buffer, offset, length);
	}

	setBlockOptions(block, ...options) {
		this.#block_options.set(block, options);
	}

	getBlockOptions(block) {
		return this.#block_options.get(block) || [];
	}

	get blockCount() {
		return this.#block_options.size;
	}

	get bufferCount() {
		return this.#segment_count;
	}

	#writeToBlock(block, block_offset, buffer, offset, length) {
		let segments = this.#blocks.get(block) || [];
		this.#segment_count -= segments.length;
		let remove_start = binarySearchArray(segments, ([segment_offset]) => (segment_offset >= block_offset));
		let remove_end = binarySearchArray(segments, ([segment_offset, segment_buffer]) => (segment_offset + segment_buffer.length > block_offset + length));
		if (remove_start < remove_end) {
			segments.splice(remove_start, remove_end - remove_start);
		}
		let out_start = block_offset;
		let out_end = block_offset + length;
		remove_end = remove_start;
		if (remove_start < segments.length) {
			const [segment_offset, segment_buffer] = segments[remove_start];
			if (segment_offset <= block_offset + length) {
				out_end = segment_offset + segment_buffer.length;
				remove_end++;
			}
		}
		if (remove_start > 0) {
			const [segment_offset, segment_buffer] = segments[remove_start - 1];
			if (segment_offset + segment_buffer.length >= block_offset) {
				out_start = segment_offset;
				remove_start--;
			}
		}
		const out = Buffer.alloc(out_end - out_start);
		if (out_start < block_offset) {
			const [segment_offset, segment_buffer] = segments[remove_start];
			segment_buffer.copy(out, 0, 0, block_offset - out_start);
		}
		if (out_end > block_offset + length) {
			const [segment_offset, segment_buffer] = segments[remove_end - 1];
			segment_buffer.copy(out, block_offset + length - out_start, block_offset + length - segment_offset, segment_buffer.length);
		}
		buffer.copy(out, block_offset - out_start, offset, offset + length);
		segments.splice(remove_start, remove_end - remove_start, [out_start, out]);
		this.#segment_count += segments.length;
		this.#blocks.set(block, segments)
	}

	#applyBlock(block, block_offset, buffer, offset, length) {
		const segments = this.#blocks.get(block);
		if (!segments) {
			return;
		}
		for (let i = binarySearchArray(segments, ([segment_offset, segment_buffer]) => (segment_offset + segment_buffer.length > block_offset)); i < segments.length; i++) {
			const [segment_offset, segment_buffer] = segments[i];
			if (segment_offset >= block_offset + length) {
				break;
			}
			const buffer_start = (segment_offset < block_offset)?0:segment_offset - block_offset;
			const segment_start = (segment_offset < block_offset)?block_offset - segment_offset:0;
			const segment_end = (segment_offset + segment_buffer.length < block_offset + length)?segment_buffer.length:(length + block_offset - segment_offset);
			segment_buffer.copy(buffer, offset + buffer_start, segment_start, segment_end);
		}
	}
}

async function navigatePath(dirent, ...path) {
	for (const frag of path) {
		if (!dirent.isDirectory()) {
			return null;
		}
		let found = null;
		for await (const subdirent of dirent.open()) {
			if (subdirent.name === frag) {
				found = subdirent;
				break;
			}
		}
		if (found === null) {
			return null;
		}
		dirent = found;
	}
	return dirent;
}

async function readAsBuffer(file) {
	const ret = Buffer.alloc(Number((await file.stat()).size));
	const res = await file.read(ret, 0, ret.length, 0);
	return ret.subarray(0, res);
}

async function hashFile(hash, file, position, length) {
	position = BigInt(position || 0n);
	const buffer = Buffer.alloc(512);
	if (!length) {
		while (true) {
			const res = await file.read(buffer, 0, 512, Number(position));
			if (res < 512) {
				if (res > 0) {
					hash.update(buffer.subarray(0, res));
				}
				return;
			}
			hash.update(buffer);
			position += 512n;
		}
	}
	while (length >= 512) {
		const res = await file.read(buffer, 0, 512, Number(position));
		if (res < 512) {
			if (res > 0) {
				hash.update(buffer.subarray(0, res));
			}
			return;
		}
		hash.update(buffer);
		position += 512n;
		length -= 512;
	}
	if (length > 0) {
		const res = await file.read(buffer, 0, length, Number(position));
		if (res > 0) {
			hash.update(buffer.subarray(0, res));
		}
	}
}

function prefixHash(prefix, buffer) {
	const hash = createHash('sha512');
	hash.update(prefix.padEnd(128, '\0'));
	hash.update(buffer);
	return hash.digest();
}

async function prefixHashFile(prefix, ...path) {
	const hash = createHash('sha512');
	hash.update(prefix.padEnd(128, '\0'));
	await hashFile(hash, (await navigatePath(...path)).open());
	return hash.digest();
}

async function decryptKey(encrypted_key_file, keymaster_key_blob_file, appid) {
	let has_prefix = false;
	const keymaster_key = Buffer.alloc(32);
	if (await keymaster_key_blob_file.read(keymaster_key, 0, 5, 0) < 5) {
		throw new Error('keymaster_key_blob is invalid');
	}
	// A typical keymaster key has the following contents:
	// 00 - Version
	// 20 00 00 00 - Key length
	// u8[32] - Key contents <- The only part we actually need
	// 00 00 00 00 - Hardware enforced byte contents length
	// u8[0] - No byte contents
	// 00 00 00 00 - Hardware enforced parameter count
	// 00 00 00 00 - Hardware enforced parameter size
	// u8[0] - No parameters
	// 00 00 00 00 - Software enforced byte contents length
	// u8[0] - No byte contents
	// 0E 00 00 00 - Software enforced parameter count
	// 71 00 00 00 - Software enforced parameter size
	// 02 00 00 10 - Algorithm =
	// 20 00 00 00 - AES
	// 03 00 00 30 - Key Size =
	// 00 01 00 00 - 256
	// 01 00 00 20 - Purpose +=
	// 00 00 00 00 - Encrypt
	// 01 00 00 20 - Purpose +=
	// 01 00 00 00 - Decrypt
	// 04 00 00 20 - Block mode =
	// 20 00 00 00 - GCM
	// 06 00 00 20 - Padding mode =
	// 01 00 00 00 - None
	// 08 00 00 30 - MAC length =
	// 80 00 00 00 - 128
	// F7 01 00 70 - No authentication required =
	// 01 - True
	// BD 02 00 60 - Creation datetime =
	// u64le - Milliseconds since epoch
	// BE 02 00 10 - Origin =
	// 00 00 00 00 - Generated
	// C1 02 00 30 - OS version =
	// u32le - Version number
	// C2 02 00 30 - OS patch level =
	// u32le - Version number
	// CE 02 00 30 - Vendor patch level =
	// u32le - Version number
	// CF 02 00 30 - Boot patch level =
	// u32le - Version number
	// u8[8] - HMAC signature to verify both this file's contents and the caller
	//
	// We don't need to read any of the above other than the key contents
	// If a different algorithm is specified, the code below doesn't support it anyway
	// In that case, the decryption will simply fail, so no need to check anything else
	if (!keymaster_key.subarray(0, 5).equals(Buffer.from([0, 32, 0, 0, 0]))) {
		// Check for optional prefix 'pKMblob\0'
		if (keymaster_key.subarray(0, 5).equals(Buffer.from([0x70, 0x4B, 0x4D, 0x62, 0x6C]))) {
			if (await keymaster_key_blob_file.read(keymaster_key, 5, 8, 5) >= 8) {
				if (keymaster_key.subarray(5, 13).equals(Buffer.from([0x6F, 0x62, 0x00, 0, 32, 0, 0, 0]))) {
					has_prefix = true;
				}
			}
		}
		if (!has_prefix) {
			throw new Error('keymaster_key_blob is invalid or unsupported key algorithm is used');
		}
	}
	if (appid) {
		// We could technically skip this verification step
		// If the key is corrupt in any way, the decryption below would fail anyway
		const hmac = createHmac('sha256', 'IntegrityAssuredBlob0\x00');
		let pos = has_prefix?8n:0n;
		while (true) {
			const res = await keymaster_key_blob_file.read(keymaster_key, 0, 32, Number(pos));
			if (res < 32) {
				if (res < 8) {
					throw new Error('Unexpected end of file reading keymaster_key_blob');
				}
				if (res > 8) {
					hmac.update(keymaster_key.subarray(0, res - 8));
					if (await keymaster_key_blob_file.read(keymaster_key, 0, 8, Number(pos + BigInt(res - 8))) < 8) {
						throw new Error('Unexpected end of file reading keymaster_key_blob');
					}
				}
				break;
			}
			hmac.update(keymaster_key.subarray(0, 24));
			pos += 24n;
		}
		if (appid === true) {
			// Key without appid
			hmac.update(Buffer.from([
				0x02, 0x00, 0x00, 0x00,	// Byte contents length
				0x53, 0x57,	// Byte contents: 'SW'
				0x01, 0x00, 0x00, 0x00,	// Parameter count
				0x0C, 0x00, 0x00, 0x00,	// Parameter size
				0xC0, 0x02, 0x00, 0x90,	// Root of trust +=
				0x02, 0x00, 0x00, 0x00,	// Byte contents subarray(Length: 2,
				0x00, 0x00, 0x00, 0x00	// Offset: 0) ('SW')
			]));
		}
		else {
			const hiddenparams = Buffer.concat([
				Buffer.from([0x00, 0x00, 0x00, 0x00]),	// Byte content length placeholder
				appid,	// Byte contents: appid
				Buffer.from([
					0x53, 0x57,	// Byte contents: 'SW'
					0x02, 0x00, 0x00, 0x00,	// Parameter count
					0x18, 0x00, 0x00, 0x00,	// Parameter size
					0x59, 0x02, 0x00, 0x90,	// Application id =
					0x00, 0x00, 0x00, 0x00,	// Byte contents subarray(Length: Application id length placeholder
					0x00, 0x00, 0x00, 0x00,	// Offset: 0)
					0xC0, 0x02, 0x00, 0x90,	// Root of trust +=
					0x02, 0x00, 0x00, 0x00,	// Byte contents subarray(Length: 2,
					0x00, 0x00, 0x00, 0x00	// Offset: Root of trust offset placeholder) ('SW')
				])
			]);
			hiddenparams.writeUInt32LE(appid.length + 2, 0);	// Byte content length
			hiddenparams.writeUInt32LE(appid.length, appid.length + 18);	// Application id length
			hiddenparams.writeUInt32LE(appid.length, appid.length + 34);	// Root of trust offset
			hmac.update(hiddenparams);
		}
		const signature = hmac.digest().subarray(0, 8);
		if (!signature.equals(keymaster_key.subarray(0, 8))) {
			throw new Error('keymaster_key_blob signature did not match');
		}
	}
	if (await keymaster_key_blob_file.read(keymaster_key, 0, 32, has_prefix?13:5) < 32) {
		throw new Error('keymaster_key_blob is invalid');
	}
	const encrypted_key = await readAsBuffer(encrypted_key_file);
	if (encrypted_key.length < 28) {
		throw new Error('encrypted_key is invalid');
	}
	const decipher = createDecipheriv('aes-256-gcm', keymaster_key, encrypted_key.subarray(0, 12));
	decipher.setAuthTag(encrypted_key.subarray(encrypted_key.length - 16, encrypted_key.length));
	return Buffer.concat([decipher.update(encrypted_key.subarray(12, encrypted_key.length - 16)), decipher.final()]);
}

function decryptCts(cipher, key) {
	if (cipher.length <= 16) {
		// Special case: Single block. Nothing to swizzle
		const ecb = createDecipheriv('aes-256-ecb', key, Buffer.alloc(0));
		ecb.setAutoPadding(false);
		return ecb.update(cipher);
	}
	const cts_cipher = Buffer.alloc(32);
	const decipher = createDecipheriv('aes-256-cbc', key, Buffer.alloc(16));
	decipher.setAutoPadding(false);
	const swizzle_start = ((cipher.length + 15) & ~15) - 16;
	cipher.copy(cts_cipher, 0, swizzle_start, cipher.length);
	cipher.copy(cts_cipher, 16, swizzle_start - 16, swizzle_start);
	const plaintext_prefix = (swizzle_start > 16)?decipher.update(cipher.subarray(0, swizzle_start - 16)):Buffer.alloc(0);
	const swizzle_offset = cipher.length & 15;
	if (swizzle_offset > 0) {
		const ecb = createDecipheriv('aes-256-ecb', key, Buffer.alloc(0));
		ecb.setAutoPadding(false);
		ecb.update(cipher.subarray(swizzle_start - 16, swizzle_start)).copy(cts_cipher, swizzle_offset + 16, swizzle_offset, 16);
	}
	return Buffer.concat([plaintext_prefix, decipher.update(cts_cipher)]).subarray(0, cipher.length);
}

// Qemu emulator stores images as Qcow2
// It's possible to supply raw images instead using BlockDevFile.open(await fopen('image'))
for await (const dev of using(await BlockDevQcow2.open('encryptionkey.img.qcow2'))) {
	const gpt = await GPT.open(await BlockDevFile.open(new FileHandleBlockDev(dev), 512));
	if (gpt.partitions.length < 1 || gpt.partitions[0].name !== 'metadata') {
		throw new Error('metadata partition not found');
	}
	const ext4 = await FileSystemExt4.open(new FileHandleBlockDev(gpt.partitions[0].data));
	const key_root = await navigatePath(ext4.root, 'vold', 'metadata_encryption', 'key');
	if (key_root === null || !key_root.isDirectory()) {
		throw new Error('vold/metadata_encryption/key not found');
	}
	const keymaster_key_blob = await navigatePath(key_root, 'keymaster_key_blob');
	if (keymaster_key_blob === null || keymaster_key_blob.isDirectory()) {
		throw new Error('keymaster_key_blob file not found');
	}
	const encrypted_key = await navigatePath(key_root, 'encrypted_key');
	if (encrypted_key === null || encrypted_key.isDirectory()) {
		throw new Error('encrypted_key file not found');
	}
	const decrypted_key = await decryptKey(encrypted_key.open(), keymaster_key_blob.open(), await prefixHashFile('Android secdiscardable SHA512', key_root, 'secdiscardable'));
	for await (const encrypted_dev of using(await BlockDevQcow2.open('userdata-qemu.img.qcow2'))) {
		// Perform metadata decryption
		// For Android version 10, everything up to this point can be skipped,
		// using the image directly instead
		const decrypting_dev = new BlockDevDecrypt(await BlockDevFile.open(new FileHandleBlockDev(encrypted_dev), 4096), decrypted_key);
		// FIXME: Also support the F2FS file system
		const ext4_data = await FileSystemExt4.open(new FileHandleBlockDev(decrypting_dev));
		ext4_data.addKey(await decryptKey((await navigatePath(ext4_data.root, 'unencrypted', 'key', 'encrypted_key')).open(), (await navigatePath(ext4_data.root, 'unencrypted', 'key', 'keymaster_key_blob')).open(), await prefixHashFile('Android secdiscardable SHA512', ext4_data.root, 'unencrypted', 'key', 'secdiscardable')));
		ext4_data.addKey(await decryptKey((await navigatePath(ext4_data.root, 'misc', 'vold', 'user_keys', 'de', '0', 'encrypted_key')).open(), (await navigatePath(ext4_data.root, 'misc', 'vold', 'user_keys', 'de', '0', 'keymaster_key_blob')).open(), await prefixHashFile('Android secdiscardable SHA512', ext4_data.root, 'misc', 'vold', 'user_keys', 'de', '0', 'secdiscardable')));
		let sp_handle = '';
		const locksettings_db = await SQLiteDatabase.open((await navigatePath(ext4_data.root, 'system', 'locksettings.db')).open());
		for await (const row of locksettings_db.getTableRows('locksettings')) {
			const [_id, name, filter, value] = await iteratorToArrayAsync(row(), 4);
			let name_string;
			for await (const file of using(await name.open())) {
				name_string = name.encoding_decoder(await readAsBuffer(file));
			}
			if (name_string !== 'sp-handle') {
				continue;
			}
			let value_string;
			for await (const file of using(await value.open())) {
				value_string = value.encoding_decoder(await readAsBuffer(file));
			}
			sp_handle = (BigInt(value_string) & 0xFFFFFFFFFFFFFFFFn).toString(16);
			break;
		}
		if (sp_handle === '') {
			// Legacy system. No synthetic password means the CE key has a regular keymaster key
			ext4_data.addKey(await decryptKey((await navigatePath(ext4_data.root, 'misc', 'vold', 'user_keys', 'ce', '0', 'current', 'encrypted_key')).open(), (await navigatePath(ext4_data.root, 'misc', 'vold', 'user_keys', 'ce', '0', 'current', 'keymaster_key_blob')).open(), await prefixHashFile('Android secdiscardable SHA512', ext4_data.root, 'misc', 'vold', 'user_keys', 'ce', '0', 'current', 'secdiscardable')));
		}
		else {
			const spblob = await readAsBuffer((await navigatePath(ext4_data.root, 'system_de', '0', 'spblob', sp_handle.padStart(16, '0') + '.spblob')).open());
			if (spblob.length < 58 || spblob.readUInt16BE() !== 0x0300) {
				// Must be version 3 LSKF
				throw new Error('Incompatible spblob file');
			}
			// Storage of the device-bound key in persistent.sqlite was added in Android 12
			// Prior to it, the key was stored in '/misc/keystore/user_0/1000_USRSKEY_synthetic_password_' + sp_handle.padStart(16, '0') instead
			const persistent_sqlite = await SQLiteDatabase.open((await navigatePath(ext4_data.root, 'misc', 'keystore', 'persistent.sqlite')).open());
			let synthetic_password_key_id = 0;
			for await (const row of persistent_sqlite.getTableRows('keyentry')) {
				const [id, key_type, domain, namespace, alias] = await iteratorToArrayAsync(row(), 5);
				let alias_string;
				for await (const file of using(await alias.open())) {
					alias_string = alias.encoding_decoder(await readAsBuffer(file));
				}
				if (alias_string !== 'synthetic_password_' + sp_handle) {
					continue;
				}
				synthetic_password_key_id = id.value;
				break;
			}
			// In version 1, the order of decryption is opposite:
			// Using the inner key first, and the key from the database second
			// In versions 2 and 3, the inner key comes second
			let spblob_decrypt_1 = null;
			for await (const row of persistent_sqlite.getTableRows('blobentry')) {
				const [id, subcomponent_type, keyentryid, blob] = await iteratorToArrayAsync(row(), 4);
				if (keyentryid.value !== synthetic_password_key_id) {
					continue;
				}
				for await (const file of using(await blob.open())) {
					spblob_decrypt_1 = await decryptKey(new FileHandleBuffer(spblob.subarray(2)), file, true);
				}
				break;
			}
			if (spblob_decrypt_1 === null) {
				throw new Error('Could not find handle to decrypt synthetic password blob');
			}
			// If a PIN is set, then 'default-password' would be replaced with an scrypt hash of the PIN
			// The salt and parameters for scrypt would need to be read from '/system_de/0/spblob/' + sp_handle.padStart(16, '0') + '.pwd'
			const inner_key = prefixHash('application-id', Buffer.concat([Buffer.from('default-password'.padEnd(32, '\0')), await prefixHashFile('secdiscardable-transform', ext4_data.root, 'system_de', '0', 'spblob', sp_handle.padStart(16, '0') + '.secdis')])).subarray(0, 32);
			const spblob_decrypt_2 = await decryptKey(new FileHandleBuffer(spblob_decrypt_1), new FileHandleBuffer(Buffer.concat([Buffer.from('0020000000', 'hex'), inner_key])));
			// Version 3 key derivation uses NIST SP800-108 in counter mode
			// The following is a single iteration for the label 'fbe-key'
			// Versions 1 and 2 simply use prefixHash('fbe-key', spblob_decrypt_2)
			const fbe_hmac = createHmac('sha256', spblob_decrypt_2);
			fbe_hmac.update(Buffer.from(
				'00000001' + // Counter
				'6662652d6b6579' + // 'fbe-key'
				'00' +
				'616e64726f69642d73796e7468657469632d70617373776f72642d706572736f6e616c697a6174696f6e2d636f6e74657874' + // 'android-synthetic-password-personalization-context'
				'00000190' + // Context length (in bits)
				'00000100', // Output length (in bits)
				'hex'));
			const fbe_key = fbe_hmac.digest();
			// If /misc/vold/user_keys/ce/0/current/secdiscardable is present, it would need to be mixed with fbe_key as
			// fbe_key = prefixHashFile('Android secdiscardable SHA512', secdiscardable) + fbe_hmac.digest()
			// However, it is usually absent if no PIN is set
			ext4_data.addKey(await decryptKey((await navigatePath(ext4_data.root, 'misc', 'vold', 'user_keys', 'ce', '0', 'current', 'encrypted_key')).open(), new FileHandleBuffer(Buffer.concat([Buffer.from('0020000000', 'hex'), prefixHash('Android key wrapping key generation SHA512', fbe_key).subarray(0, 32)]))));
		}

		// console.log is surprisingly time-consuming. This should throttle it
		let last_done = new Date();
		let should_do = null;
		function rateLimit(func) {
			const now = new Date();
			if (now - last_done < 100) {
				should_do = func;
				return;
			}
			last_done = now;
			should_do = null;
			func();
		}
		function rateLimitFlush() {
			should_do && should_do();
			should_do = null;
		}

		const change_logger = new ChangeLogger(decrypting_dev.blockSize);
		console.log();
		console.log();
		for await (const [inode, total] of ext4_data.decrypt(change_logger)) {
			rateLimit(() => {
				console.log('\x1b[A\x1b[A\x1b[2KDecrypting', inode, 'of', total, 'inodes');
				console.log('\x1b[2KChanged', change_logger.blockCount, 'blocks and', change_logger.bufferCount, 'buffers');
			});
		}
		rateLimitFlush();
		for await (const outfile of using(await fopen('userdata-decrypted.img', 'w'))) {
			const blankbuffer = Buffer.alloc(decrypting_dev.blockSize);
			await outfile.truncate(Number(decrypting_dev.blockCount * BigInt(decrypting_dev.blockSize)));
			console.log();
			for (const i of range(0n, decrypting_dev.blockCount)) {
				const block = await decrypting_dev.readBlock(i, ...change_logger.getBlockOptions(i));
				change_logger.apply(block, 0, block.length, BigInt(block.length) * i);
				if (!block.equals(blankbuffer)) {	// Skip the write if 0-filled
					await outfile.write(block, 0, block.length, Number(BigInt(block.length) * i));
				}
				rateLimit(() => {
					console.log('\x1b[A\x1b[2KWritten', i + 1n, 'of', decrypting_dev.blockCount, 'blocks');
				});
			}
		}
		rateLimitFlush();
	}
}
