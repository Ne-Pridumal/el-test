import { once } from 'node:events';
import { get as getHttp } from 'node:http';
import { get as getHttps } from 'node:https';
import { performance } from 'node:perf_hooks';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

function readPositiveInteger(name, fallback, max) {
	const rawValue = process.env[name];
	if (rawValue === undefined) return fallback;

	const value = Number(rawValue);
	if (!/^\d+$/.test(rawValue) || value < 1 || value > max) {
		throw new Error(`${name} must be an integer between 1 and ${max}.`);
	}
	return value;
}

async function download(url, get, options) {
	const { shutdownSignal, inactivityTimeoutMs, headers } = options;
	const timeout = new AbortController();
	const signal = AbortSignal.any([shutdownSignal, timeout.signal]);
	const timer = setTimeout(() => {
		timeout.abort(new Error(`Download timed out after ${inactivityTimeoutMs / 1000} seconds without receiving data.`));
	}, inactivityTimeoutMs);

	try {
		const request = get(url, { signal, headers });
		const [response] = await once(request, 'response');
		// The body pipeline handles cancellation after the response arrives.
		request.on('error', () => { });
		timer.refresh();

		let bytes = 0;
		const counter = new Writable({
			write(chunk, _, done) {
				timer.refresh();
				bytes += chunk.length;
				done();
			},
		});
		await pipeline(response, counter, { signal });
		signal.throwIfAborted();

		if (response.statusCode < 200 || response.statusCode >= 300) {
			throw new Error(`Server returned HTTP ${response.statusCode}`);
		}
		return bytes;
	} catch (error) {
		throw signal.reason ?? error;
	} finally {
		clearTimeout(timer);
	}
}

function formatVolume(bytes) {
	const units = [[1e9, 'GB'], [1e6, 'MB'], [1e3, 'KB']];
	const [divisor, unit] = units.find(([size]) => bytes >= size) ?? units.at(-1);
	return `${(bytes / divisor).toFixed(3)} ${unit}`;
}

async function main(signal) {
	const [address, ...extraArgs] = process.argv.slice(2);
	const url = URL.canParse(address ?? '') ? new URL(address) : null;
	if (extraArgs.length || !url || !['http:', 'https:'].includes(url.protocol)) {
		throw new Error('Expected one HTTP or HTTPS URL.');
	}

	const requestCount = readPositiveInteger('REQUEST_COUNT', 10, 2 ** 32 - 1);
	const inactivityTimeoutMs = readPositiveInteger('INACTIVITY_TIMEOUT_MS', 15_000, 2 ** 31 - 1);
	const get = url.protocol === 'https:' ? getHttps : getHttp;
	const userAgent = process.env.USER_AGENT || 'download-speed/1.0';
	const options = { shutdownSignal: signal, inactivityTimeoutMs, headers: { 'User-Agent': userAgent } };
	const { totalBytes, totalMs } = await Array.from({ length: requestCount }).reduce(
		async (previous, _, index) => {
			const totals = await previous;
			signal.throwIfAborted();
			console.error(`Downloading ${index + 1}/${requestCount} (${index} completed)...`);
			const startedAt = performance.now();
			const bytes = await download(url, get, options);
			return {
				totalBytes: totals.totalBytes + bytes,
				totalMs: totals.totalMs + (performance.now() - startedAt),
			};
		},
		{ totalBytes: 0, totalMs: 0 },
	);

	signal.throwIfAborted();
	console.error(`Completed ${requestCount}/${requestCount} requests.`);
	const bytesPerSecond = totalBytes / (totalMs / 1000);
	console.log(`Average request time: ${(totalMs / requestCount).toFixed(2)} ms`);
	console.log(`Total downloaded: ${formatVolume(totalBytes)}`);
	console.log(`Download speed: ${formatVolume(bytesPerSecond)}/s`);
}

const controller = new AbortController();
const exitCodes = { SIGINT: 130, SIGTERM: 143 };

function shutdown(signalName) {
	if (controller.signal.aborted) return;

	process.exitCode = exitCodes[signalName];
	console.error(`Received ${signalName}. Stopping the benchmark.`);
	controller.abort();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

try {
	await main(controller.signal);
} catch (error) {
	if (!controller.signal.aborted) {
		console.error(error.message);
		process.exitCode = 1;
	}
} finally {
	process.off('SIGINT', shutdown);
	process.off('SIGTERM', shutdown);
}
