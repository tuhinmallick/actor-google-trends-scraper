const { Actor } = require('apify');
const {
    PuppeteerCrawler,
    RequestQueue,
    Dataset,
    log,
    sleep,
    puppeteerUtils,
    createRequestDebugInfo,
} = require('crawlee');

const {
    validateInput,
    checkAndCreateUrlSource,
    maxItemsCheck,
    checkAndEval,
    applyFunction,
    proxyConfiguration,
} = require('./utils');

Actor.main(async () => {
    /** @type {any} */
    const input = await Actor.getInput();
    validateInput(input);

    const {
        searchTerms,
        spreadsheetId,
        isPublic = false,
        timeRange,
        category,
        maxItems = null,
        customTimeRange = null,
        geo = null,
        extendOutputFunction = null,
        pageLoadTimeoutSecs = 180,
        maxConcurrency = 10,
        outputAsISODate = false,
    } = input;

    const proxyConfig = await proxyConfiguration({
        proxyConfig: input.proxyConfiguration,
    });

    // initialize request list from url sources
    const { sources, sheetTitle } = await checkAndCreateUrlSource(
        searchTerms,
        spreadsheetId,
        isPublic,
        timeRange,
        category,
        customTimeRange,
        geo,
    );
    // open request queue
    const requestQueue = await RequestQueue.open();

    await requestQueue.addRequest({
        url: 'https://trends.google.com/trends', // hot start it
        userData: {
            label: 'START',
        },
    });

    // open dataset and get itemCount
    const dataset = await Dataset.open();
    let { itemCount } = await dataset.getInfo();

    // if exists, evaluate extendOutputFunction, or throw
    checkAndEval(extendOutputFunction);

    // Per-request promises for the widgetdata API response, keyed by request.uniqueKey.
    // Set up in preNavigationHooks BEFORE navigation so the response is never missed.
    const responseListeners = new Map();

    // crawler config
    const crawler = new PuppeteerCrawler({
        requestQueue,
        maxRequestRetries: 3,
        // needs some time to enqueue all the initial requests
        requestHandlerTimeoutSecs: 300,
        maxConcurrency,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: maxConcurrency,
            sessionOptions: {
                maxErrorScore: 5,
            },
            // retire sessions on 401/403 instead of the v2 monkey-patch on apify/build/constants
            blockedStatusCodes: [401, 403],
        },
        proxyConfiguration: proxyConfig,
        launchContext: {
            launchOptions: {
                args: ['--no-sandbox', '--disable-setuid-sandbox'],
            },
        },
        browserPoolOptions: {
            maxOpenPagesPerBrowser: 1,
        },
        persistCookiesPerSession: true,
        preNavigationHooks: [async ({ page, request }, gotoOptions) => {
            gotoOptions.timeout = pageLoadTimeoutSecs * 1000;
            gotoOptions.waitUntil = 'domcontentloaded';

            // For SEARCH requests, capture the internal widgetdata/multiline API response
            // before navigation starts so it's never missed if the XHR fires quickly.
            if (request.userData?.label === 'SEARCH') {
                const responsePromise = page.waitForResponse(
                    (res) => res.url().includes('/trends/api/widgetdata/multiline'),
                    { timeout: 125 * 1000 },
                ).catch(() => null); // null = timed out (no data or very slow)
                responseListeners.set(request.uniqueKey, responsePromise);
            }
        }],
        postNavigationHooks: [async ({ page }) => {
            await page.bringToFront();
        }],
        requestHandler: async ({ page, request }) => {
            // if exists, check items limit. If limit is reached crawler will exit.
            if (maxItems && maxItemsCheck(maxItems, itemCount)) {
                return;
            }

            const is429 = await page.evaluate(() => !!document.querySelector('div#af-error-container'));
            if (is429) {
                await sleep(10000);
                // eslint-disable-next-line no-throw-literal
                throw 'Page got a 429 Error. Google is baiting us to throw out the proxy but we need to stick with it...';
            }

            log.info('Processing:', { url: request.url });
            const { label } = request.userData;

            if (label === 'START') {
                for (const source of sources) {
                    await requestQueue.addRequest(source);
                }
            } else if (label === 'SEARCH') {
                if (extendOutputFunction) {
                    await puppeteerUtils.injectJQuery(page);
                }

                const queryStringObj = new URL(request.url);
                const searchTerm = queryStringObj.searchParams.get('q');
                const terms = [...(searchTerm?.split(','))];

                // Retrieve the pre-registered API response promise and clean up the map
                const responsePromise = responseListeners.get(request.uniqueKey) ?? Promise.resolve(null);
                responseListeners.delete(request.uniqueKey);

                // Detects when the TIMESERIES widget shows a "no data" error instead of a chart.
                // Resolves 'no-data' if the error title appears, or 'timeout' if neither fires.
                const noDataPromise = page.waitForSelector('[widget-name=TIMESERIES]', { timeout: 30000 })
                    .then(() => page.waitForFunction(() => {
                        const w = document.querySelector('[widget-name=TIMESERIES]');
                        return !!w?.querySelector('p.widget-error-title');
                    }, { timeout: 120 * 1000 }))
                    .then(() => ({ type: 'no-data' }))
                    .catch(() => ({ type: 'timeout' }));

                // Race: internal JSON API response (data exists) vs no-data widget detection
                const outcome = await Promise.race([
                    responsePromise.then((r) => (r ? { type: 'data', response: r } : { type: 'timeout' })),
                    noDataPromise,
                ]);

                if (outcome.type === 'no-data') {
                    const resObject = Object.create(null);
                    resObject[sheetTitle] = searchTerm;
                    resObject.message = 'The search term displays no data.';
                    await Actor.pushData({ ...resObject, ...await applyFunction(page, extendOutputFunction) });
                    log.info(`The search term "${searchTerm}" displays no data.`);
                    await puppeteerUtils.saveSnapshot(page, {
                        key: `NO-DATA-${searchTerm.replace(/[^a-zA-Z0-9-_]/g, '-')}`,
                        saveHtml: false,
                    });
                    return;
                }

                if (outcome.type !== 'data') {
                    throw new Error(`Timed out waiting for trends data for "${searchTerm}"`);
                }

                // Parse the internal API JSON — strip Google's XSSI protection prefix ")]}'\n"
                const text = await outcome.response.text();
                const json = JSON.parse(text.slice(5));

                /** @type {Array<{time: string, formattedTime: string, formattedAxisTime: string, value: number[], hasData: boolean[], formattedValue: string[]}>} */
                const timelineData = json?.default?.timelineData ?? [];

                if (!timelineData.length) {
                    const resObject = Object.create(null);
                    resObject[sheetTitle] = searchTerm;
                    resObject.message = 'The search term displays no data.';
                    await Actor.pushData({ ...resObject, ...await applyFunction(page, extendOutputFunction) });
                    log.info(`The search term "${searchTerm}" displays no data (empty timeline).`);
                    return;
                }

                const resObject = Object.create(null);
                resObject[sheetTitle] = searchTerm;

                for (const entry of timelineData) {
                    // Skip sparse entries where Google has no data for this period
                    if (!entry.hasData?.some(Boolean)) continue;

                    // formattedAxisTime matches the old DOM table format (e.g. "Nov 1, 2020")
                    // formattedTime includes the full range (e.g. "Nov 1 – 7, 2020") as fallback
                    const key = outputAsISODate
                        ? new Date(Number(entry.time) * 1000).toISOString()
                        : (entry.formattedAxisTime || entry.formattedTime);

                    resObject[key] = terms.map((_, i) => entry.value[i] ?? 0).join(',');
                }

                const result = await applyFunction(page, extendOutputFunction);

                // push, increase itemCount, log
                await Actor.pushData({ ...resObject, ...result });

                itemCount++;
                log.info(`Results for "${searchTerm}" pushed successfully.`);
            }
        },

        failedRequestHandler: async ({ request }) => {
            log.warning(`Request ${request.url} failed too many times`);

            await dataset.pushData({
                '#debug': createRequestDebugInfo(request),
            });
        },
    });

    log.info('Starting crawler.');
    await crawler.run();

    log.info('Crawler Finished.');

    if (spreadsheetId) {
        log.info(`
            You can get the results as usual,
            or download them in spreadsheet format under the 'dataset' tab of your actor run.
            More info in the actor documentation.
        `);
    }
});
