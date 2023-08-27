// Package api provides types used by the Linkbox API.
package api

type Entity struct {
	Type    string `json:"type"`
	SubType string `json:"sub_type"`
	Name    string `json:"name"`
	URL     string `json:"url"`
	Ctime   int64  `json:"ctime"`
	Size    int    `json:"size"`
	ID      int    `json:"id"`
	Pid     int    `json:"pid"`
	ItemID  string `json:"item_id"`
}
type Data struct {
	Entities []Entity `json:"list"`
}
type FileSearchRes struct {
	SearchData Data   `json:"data"`
	Status     int    `json:"status"`
	Message    string `json:"msg"`
}

type LoginRes struct {
	Data struct {
		Avatar       string `json:"avatar"`
		Nickname     string `json:"nickname"`
		OpenID       string `json:"openId"`
		RefreshToken string `json:"refresh_token"`
		Token        string `json:"token"`
		UID          int    `json:"uid"`
		UserInfo     struct {
			APIKey         string `json:"api_key"`
			AutoRenew      bool   `json:"auto_renew"`
			Avatar         string `json:"avatar"`
			BuyP7          bool   `json:"buy_p7"`
			Email          string `json:"email"`
			FavFlag        bool   `json:"fav_flag"`
			GroupPLimitCnt int    `json:"group_p_limit_cnt"`
			IsTry          bool   `json:"is_try"`
			Nickname       string `json:"nickname"`
			OpenID         string `json:"open_id"`
			SetPrivacy     int    `json:"set_privacy"`
			SizeCap        int64  `json:"size_cap"`
			SizeCurr       int    `json:"size_curr"`
			State          int    `json:"state"`
			SubAutoRenew   int    `json:"sub_auto_renew"`
			SubOrderID     int    `json:"sub_order_id"`
			SubPid         int    `json:"sub_pid"`
			SubSource      int    `json:"sub_source"`
			SubType        int    `json:"sub_type"`
			UID            int    `json:"uid"`
			VipEnd         int    `json:"vip_end"`
			VipLv          int    `json:"vip_lv"`
		} `json:"userInfo"`
	} `json:"data"`
	Status int `json:"status"`
}

type CommonResponse struct {
	Message string `json:"msg"`
	Status  int    `json:"status"`
}

type UploadUrlData struct {
	SignUrl string `json:"signUrl"`
}

type UploadUrlResponse struct {
	Data UploadUrlData `json:"data"`
	CommonResponse
}

type UploadFileResponse struct {
	Data struct {
		ItemID string `json:"itemId"`
	} `json:"data"`
	Msg    string `json:"msg"`
	Status int    `json:"status"`
}
