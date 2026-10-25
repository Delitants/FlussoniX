export function managementOriginAllowed(protocol: string, hostname: string): boolean {
  if (protocol.toLowerCase() === 'https:') return true;
  if (protocol.toLowerCase() !== 'http:') return false;
  const host = hostname.toLowerCase();
  if (host === 'localhost' || host === '[::1]' || host === '::1') return true;
  const octets = host.split('.');
  return octets.length === 4
    && octets[0] === '127'
    && octets.slice(1).every(octet => /^(0|[1-9][0-9]{0,2})$/.test(octet) && Number(octet) <= 255);
}
