import type { SiteNetwork } from "./site-analytics.constants";

/**
 * The autonomous systems the site statistic counts as data centres: clouds and
 * hosters, whose addresses run machines rather than people's browsers. A
 * headless browser with an ordinary User-Agent passes `isBotUserAgent`; its
 * address still says where it runs. Each number with its holder as RIPEstat
 * names it. Kept by hand: a network goes on in a pull request, with a test.
 *
 * A data centre's hit is kept and marked, never dropped, and the reports leave
 * it out unless asked (`DatacenterScope`). {@link NEVER_DATACENTER} lists what
 * must not join: networks whose traffic is mostly people.
 */
export const DATACENTER_ASNS: ReadonlyMap<number, string> = new Map([
  [16509, "Amazon (AMAZON-02)"],
  [14618, "Amazon (AMAZON-AES)"],
  [8987, "Amazon GovCloud"],
  [38895, "Amazon (AMAZON-AS-AP)"],
  [15169, "Google"],
  [19527, "Google (GOOGLE-2)"],
  [396982, "Google Cloud Platform"],
  [8075, "Microsoft / Azure"],
  [31898, "Oracle Cloud"],
  [45102, "Alibaba Cloud"],
  [37963, "Alibaba Cloud (Hangzhou)"],
  [132203, "Tencent Cloud"],
  [45090, "Tencent Cloud (Shenzhen)"],
  [136907, "Huawei Cloud"],
  [55990, "Huawei Cloud (data center)"],
  [36351, "IBM Cloud (SoftLayer)"],
  [14061, "DigitalOcean"],
  [24940, "Hetzner Online"],
  [213230, "Hetzner Cloud"],
  [212317, "Hetzner Cloud"],
  [16276, "OVH"],
  [63949, "Akamai Connected Cloud (Linode)"],
  [20473, "Vultr"],
  [51167, "Contabo"],
  [12876, "Scaleway"],
  [60781, "Leaseweb Netherlands"],
  [28753, "Leaseweb Deutschland"],
  [16265, "Leaseweb Network"],
  [30633, "Leaseweb USA"],
  [8560, "IONOS"],
  [6724, "Strato"],
  [197540, "netcup"],
  [47583, "Hostinger"],
  [26496, "GoDaddy"],
  [19994, "Rackspace"],
  [27357, "Rackspace"],
  [54825, "Packet (Equinix Metal)"],
]);

/**
 * Networks that stay off {@link DATACENTER_ASNS} whatever runs on them: Apple's
 * iCloud Private Relay leaves through Akamai, Cloudflare and Fastly, Cloudflare
 * WARP through Cloudflare, and VPN providers rent M247's and Datacamp's racks.
 * Their readers are people, so counting them as machines would undercount
 * exactly the privacy-minded ones.
 */
export const NEVER_DATACENTER: ReadonlyMap<number, string> = new Map([
  [13335, "Cloudflare (Private Relay, WARP)"],
  [54113, "Fastly (Private Relay)"],
  [36183, "Akamai (Private Relay)"],
  [20940, "Akamai"],
  [9009, "M247 (VPN providers)"],
  [60068, "Datacamp / CDN77 (VPN providers)"],
  [714, "Apple"],
]);

/**
 * The network of a reader's address: `unknown` without the ASN database,
 * `datacenter` for an autonomous system on {@link DATACENTER_ASNS}, `other`
 * for every other one and for an address the database does not place.
 */
export function networkOf(
  asnDatabaseLoaded: boolean,
  asn: number | null,
): SiteNetwork {
  if (!asnDatabaseLoaded) return "unknown";
  return asn !== null && DATACENTER_ASNS.has(asn) ? "datacenter" : "other";
}
