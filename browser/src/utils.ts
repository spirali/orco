

export function formatSize(value : number): string
{
    if (value < 512) {
        return value + " B";
    }
    if (value < 524288) {
        return (value / 1024).toFixed(2) + " KiB"
    }
    if (value < 536870912) {
        return (value / 1024 / 1024).toFixed(2) + " MiB"
    }
    return (value / 1024 / 1024 / 1024).toFixed(2) + " GiB"
}

export default formatSize;