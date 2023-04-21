function parseModeUrl(url: URL) {
    if (url.pathname.includes('/spaces/')) {
        const collectionId = url.pathname.split('/spaces/')[1].split('/')[0]
        console.log('look here please', collectionId)
        return {
            facets: qualifiedNameFacets(`${tenant}/mode`, `/${collectionId}`),
            typeName: 'ModeCollection',
@@ -510,7 +509,6 @@ function parseModeUrl(url: URL) {
    }
    if (url.pathname.includes('/reports/')) {
        const reportId = url.pathname.split('/reports/')[1].split('/')[1]
        console.log('report Id', reportId, url.pathname.split('/reports/')[1])
        return {
            facets: qualifiedNameFacets(`${tenant}/mode`, `/${reportId}`),
            typeName: 'ModeReport',
@@ -577,6 +575,64 @@ const parseSigmaUrl = (link, sigmaSelectedPage) => {
    return null
}

const parseQuicksightUrl = (link: URL) => {
    let facets: Facets
    let typeName: string
    if (link.pathname.includes('dashboards/')) {
        const dashboardName = link.pathname
            .split('dashboards/')[1]
            .split('/')[0]
        if (dashboardName) {
            facets = {
                properties: {
                    databaseQualifiedName: [
                        {
                            operator: 'endsWith',
                            operand: 'qualifiedName',
                            value: dashboardName,
                        },
                    ],
                },
            }
            typeName = 'QuickSightDashboard'
            return { facets, typeName }
        }
    } else if (link.pathname.includes('analyses/')) {
        const analyses = link.pathname.split('analyses/')[1].split('/')[0]
        facets = {
            properties: {
                databaseQualifiedName: [
                    {
                        operator: 'endsWith',
                        operand: 'qualifiedName',
                        value: analyses,
                    },
                ],
            },
        }
        typeName = 'QuickSightAnalysis'
        return { facets, typeName }
    } else if (link.pathname.includes('data-sets/')) {
        const datasetQualifiedName = link.pathname
            .split('data-sets/')[1]
            .split('/')[0]
        facets = {
            properties: {
                databaseQualifiedName: [
                    {
                        operator: 'endsWith',
                        operand: 'qualifiedName',
                        value: datasetQualifiedName,
                    },
                ],
            },
        }
        typeName = 'QuickSightDataset'
        return { facets, typeName }
    }
    return null
}

export function useSiteParser(metadata: RefOrValue<SiteParserMetadata>) {
    const metadataRef = computed(() => getRefValue(metadata))

@@ -615,6 +671,9 @@ export function useSiteParser(metadata: RefOrValue<SiteParserMetadata>) {
    const isSigmaUrl = computed(() =>
        parsedUrl.value?.hostname?.includes('app.sigmacomputing.com')
    )
    const isQuicksightUrl = computed(() =>
        parsedUrl.value?.hostname?.includes('quicksight.aws.amazon')
    )

    const parsed = computed(() => {
        if (parsedUrl.value == null) {
@@ -672,6 +731,12 @@ export function useSiteParser(metadata: RefOrValue<SiteParserMetadata>) {
                metadataRef.value.resourceSpecificContext
            )
            return obj
        } else if (
            isQuicksightUrl.value ||
            metadataRef.value.customType === 'quicksight'
        ) {
            const obj = parseQuicksightUrl(parsedUrl.value)
            return obj
        }
        return null
    })
