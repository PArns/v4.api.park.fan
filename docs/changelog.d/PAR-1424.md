### Added — push to the iOS app over APNs, with news push

The iOS app registers as a device (`POST /v1/push/devices`: install id, APNs
token, locale, zone, the kinds it wants, an optional park narrowing for news) and
then uses the existing ride-alert and show-follow routes with the returned
`apns:<installId>` endpoint, so every existing trigger reaches it.
`PushService.send` goes to APNs for those rows and never sends a kind the device
switched off; a token Apple calls dead (410, `BadDeviceToken`,
`DeviceTokenNotForTopic`) deletes the device. A new `news` half of the push job
announces a new post of the news export to devices with news on. Without the
`APNS_*` variables nothing changes. Web push is untouched. See
[push](frontend/push.md#ios-app-apns-v1pushdevices).
