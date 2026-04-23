import asyncio
import base64
import json
import re
from pathlib import Path
from urllib.parse import quote
from typing import Optional, List, Dict, Any, Union, Tuple
from app.log import logger
from p189client import P189Client, P189APIClient, check_response
from p189client.exception import P189OSError
from p189sign import make_encrypted_params_headers

class P189ClientWrapper:
    """
    天翼云盘客户端封装类 v1.0.4
    """
    def __init__(self, username: str = "", password: str = "", cookies: str = "", cookie_store_path: str = ""):
        self.username = username
        self.password = password
        self.cookie_store_path = str(cookie_store_path or "").strip()
        cookie_text = (cookies or "").strip()
        if not cookie_text and self.cookie_store_path:
            cookie_text = self._load_cookie_store() or ""
        if not cookie_text and password and ("UID=" in password or "SEID=" in password or "COOKIE_LOGIN_USER" in password):
            cookie_text = password.strip()
        self.client = P189Client(cookies=cookie_text)
        self.session_key: Optional[str] = None

    def _cache_session_key_from_payload(self, payload: Any):
        sk = self._pick_session_key(payload)
        if not sk:
            return
        self.session_key = sk
        try:
            self.client.__dict__["session_key"] = sk
        except Exception:
            pass
        logger.info(f"【P189Client】已缓存 sessionKey: {self._session_key_brief(sk)}")

    def _load_cookie_store(self) -> str:
        if not self.cookie_store_path:
            return ""
        try:
            p = Path(self.cookie_store_path)
            if not p.exists():
                return ""
            txt = p.read_text(encoding="utf-8").strip()
            if txt:
                logger.info("【P189Client】已加载本地会话 Cookie。")
            return txt
        except Exception as e:
            logger.warning(f"【P189Client】读取本地会话 Cookie 失败: {e}")
            return ""

    def _save_cookie_store(self):
        if not self.cookie_store_path:
            return
        try:
            cookies_str = (self.client.cookies_str or "").strip()
            if not cookies_str:
                return
            p = Path(self.cookie_store_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(cookies_str, encoding="utf-8")
        except Exception as e:
            logger.warning(f"【P189Client】保存本地会话 Cookie 失败: {e}")

    @property
    def is_logged_in(self) -> bool:
        return bool(self.client.cookies_str)

    async def login(self, force: bool = False) -> bool:
        """
        登录 189（参考 P189-Share 的多路径登录策略）
        """
        if self.is_logged_in and not force:
            return True

        if force:
            self.client = P189Client(cookies="")
            self.session_key = None

        username = (self.username or "").strip()
        password = (self.password or "").strip()

        if not username and password and "=" in password and ";" in password:
            self.client = P189Client(cookies=password)
            if self.is_logged_in:
                logger.info("【P189Client】检测到 Cookie 文本，已直接作为会话使用。")
                return True

        logger.info(f"【P189Client】正在尝试登录账号: {username}")
        errors: List[str] = []

        try:
            resp = await self.client.login(username, password, async_=True)
            self._cache_session_key_from_payload(resp)
            if isinstance(resp, dict) and resp.get("cookies"):
                self.client.cookies = resp.get("cookies") or {}
            if self.is_logged_in:
                logger.info("【P189Client】账号登录成功（primary）。")
                self._save_cookie_store()
                return True
            errors.append("primary:empty_cookies")
        except Exception as e:
            errors.append(f"primary:{type(e).__name__}:{e}")

        try:
            resp = await P189Client.login_with_password2(username, password, async_=True)
            self._cache_session_key_from_payload(resp)
            if isinstance(resp, dict):
                cookies = resp.get("cookies") or {}
                if cookies:
                    self.client.cookies = cookies
            elif isinstance(resp, str) and "=" in resp:
                self.client = P189Client(cookies=resp.strip())
            if self.is_logged_in:
                logger.info("【P189Client】账号登录成功（fallback2 login_with_password2）。")
                self._save_cookie_store()
                return True
            errors.append("fallback2:empty_cookies")
        except Exception as e:
            errors.append(f"fallback2:{type(e).__name__}:{e}")

        try:
            resp = await P189APIClient.login_with_password(username, password, async_=True)
            self._cache_session_key_from_payload(resp)
            if isinstance(resp, dict):
                cookies = resp.get("cookies") or {}
                if cookies:
                    self.client.cookies = cookies
            elif isinstance(resp, str) and "=" in resp:
                self.client = P189Client(cookies=resp.strip())
            if self.is_logged_in:
                logger.info("【P189Client】账号登录成功（fallback3 P189APIClient.login_with_password）。")
                self._save_cookie_store()
                return True
            errors.append("fallback3:empty_cookies")
        except P189OSError as e:
            msg = e.message
            to_url = None
            if isinstance(msg, dict):
                if msg.get("result") in (0, "0", None) and msg.get("toUrl"):
                    to_url = msg.get("toUrl")
            elif isinstance(msg, str):
                try:
                    parsed = json.loads(msg)
                    if isinstance(parsed, dict) and parsed.get("toUrl"):
                        to_url = parsed.get("toUrl")
                except Exception:
                    pass

            if to_url:
                try:
                    sess = await P189Client.login_session_pc(to_url, async_=True)
                    self._cache_session_key_from_payload(sess)
                    if isinstance(sess, dict):
                        sess_cookies = sess.get("cookies") or {}
                        if sess_cookies:
                            self.client.cookies = sess_cookies
                        elif sess.get("sessionKey"):
                            sso_resp = await P189Client.login_sso(sess.get("sessionKey"), async_=True)
                            self._cache_session_key_from_payload(sso_resp)
                            if isinstance(sso_resp, dict) and sso_resp.get("cookies"):
                                self.client.cookies = sso_resp.get("cookies") or {}
                    if self.is_logged_in:
                        logger.info("【P189Client】账号登录成功（fallback4 login_session_pc/login_sso）。")
                        self._save_cookie_store()
                        return True
                    errors.append("fallback4:empty_cookies")
                except Exception as se:
                    errors.append(f"fallback4:{type(se).__name__}:{se}")
            else:
                errors.append(f"fallback3_p189os:{type(e).__name__}:{e}")
        except Exception as e:
            errors.append(f"fallback3:{type(e).__name__}:{e}")

        logger.error(f"【P189Client】登录失败: {' | '.join(errors)}")
        return False

    async def ensure_logged_in(self):
        """
        确保已登录
        """
        if not self.is_logged_in:
            ok = await self.login()
            if not ok:
                raise RuntimeError("P189 login failed")

    def _pick_session_key(self, payload: Any) -> Optional[str]:
        if isinstance(payload, dict):
            for key in ("sessionKey", "sessionkey", "session_key"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            for key in ("data", "result", "res", "body"):
                value = payload.get(key)
                if isinstance(value, dict):
                    found = self._pick_session_key(value)
                    if found:
                        return found
        return None

    def _session_key_brief(self, sk: Optional[str]) -> str:
        if not isinstance(sk, str) or not sk.strip():
            return "empty"
        sk = sk.strip()
        return f"{sk[:8]}...len={len(sk)}"

    async def _peek_upload_pkid(self) -> Optional[str]:
        try:
            pkid = self.client.upload_rsa_pkid
            if isinstance(pkid, str) and pkid.strip():
                return pkid.strip()
            rsakey = self.client.upload_rsakey
            if isinstance(rsakey, dict):
                return str(rsakey.get("pkId") or rsakey.get("pkid") or "").strip() or None
        except Exception as e:
            logger.warning(f"【P189Client】读取 upload_rsakey 失败: {type(e).__name__}: {e}")
        return None

    async def _upload_request_with_pubkey(self, api: str, payload: dict) -> dict:
        session_key = await self._ensure_upload_session_key()
        if not session_key:
            raise RuntimeError("session_key_missing")

        try:
            rsakey = self.client.upload_rsakey
        except Exception as e:
            raise RuntimeError(f"upload_rsakey_missing: {type(e).__name__}: {e}") from e

        pkid = str((rsakey or {}).get("pkId") or (rsakey or {}).get("pkid") or "").strip()
        pubkey_raw = (rsakey or {}).get("pubKey")
        if not pkid or not pubkey_raw:
            raise RuntimeError("upload_rsakey_incomplete")

        pubkey_perm: Any = pubkey_raw
        if isinstance(pubkey_raw, str):
            key_text = pubkey_raw.strip()
            if key_text and not key_text.startswith("-----"):
                key_text = f"-----BEGIN PUBLIC KEY-----\n{key_text}\n-----END PUBLIC KEY-----"
            try:
                from Crypto.PublicKey import RSA
                rsa_key = RSA.import_key(key_text)
                pubkey_perm = (rsa_key.n, rsa_key.e)
            except Exception as e:
                raise RuntimeError(f"upload_pubkey_parse_failed: {type(e).__name__}: {e}") from e

        url = f"https://upload.cloud.189.cn/person/{api}"
        encrypted = make_encrypted_params_headers(
            session_key,
            url=url,
            method="GET",
            payload=payload,
            pkid=pkid,
            pubkey=pubkey_perm,
        )

        params: dict = {}
        headers: dict = {}
        if isinstance(encrypted, tuple):
            if len(encrypted) >= 1 and isinstance(encrypted[0], dict):
                params = encrypted[0]
            if len(encrypted) >= 2 and isinstance(encrypted[1], dict):
                headers = encrypted[1]
        elif isinstance(encrypted, dict):
            if isinstance(encrypted.get("params"), dict):
                params = encrypted.get("params") or {}
            elif "params" in encrypted:
                params = {"params": encrypted.get("params")}

            if isinstance(encrypted.get("headers"), dict):
                headers = encrypted.get("headers") or {}
            elif not params:
                params = encrypted

        if not params:
            raise RuntimeError("encrypted_params_missing")

        logger.info(
            f"【P189Client】加密上传请求 api={api} "
            f"session_key={self._session_key_brief(session_key)} pkid={pkid}"
        )
        return await self.client.request(url, method="GET", params=params, headers=headers, async_=True)

    async def _refresh_upload_crypto_context(self, reason: str, refresh_session_key: bool = True) -> tuple[Optional[str], Optional[str]]:
        self.client.__dict__.pop("upload_rsakey", None)
        self.client.__dict__.pop("upload_rsa_pkid", None)
        if refresh_session_key:
            self.client.__dict__.pop("session_key", None)

        session_key = await self._ensure_upload_session_key()
        pkid = await self._peek_upload_pkid()
        logger.info(
            f"【P189Client】刷新上传上下文 reason={reason} "
            f"session_key={self._session_key_brief(session_key)} pkid={pkid}"
        )
        return session_key, pkid

    def _is_session_invalid_payload(self, payload: Any, err: Optional[Exception] = None) -> bool:
        text_parts = []
        if err is not None:
            text_parts.append(str(err))
        if isinstance(payload, dict):
            # 兼容 189 各种 API 的返回字段
            text_parts.extend([
                str(payload.get("msg") or ""),
                str(payload.get("message") or ""),
                str(payload.get("res_message") or ""),
                str(payload.get("code") or ""),
                str(payload.get("res_code") or ""),
                str(payload.get("errorCode") or ""),
                str(payload.get("errorMsg") or ""),
            ])
            if isinstance(payload.get("data"), dict):
                data = payload.get("data") or {}
                text_parts.extend([
                    str(data.get("msg") or ""),
                    str(data.get("message") or ""),
                    str(data.get("res_message") or ""),
                    str(data.get("code") or ""),
                    str(data.get("res_code") or ""),
                ])

        merged = " | ".join([x for x in text_parts if x]).lower()
        
        # 诊断 IP 变动
        if "check ip error" in merged:
            logger.warning("【P189Client】检测到出口 IP 变动导致的会话失效 (check ip error)，建议固定容器出口 IP。")
            
        return (
            ("decrypt encryptiontext failed" in merged) 
            or ("invalidsessionkey" in merged) 
            or ("cookieusersession" in merged and "invalid" in merged)
            or ("session" in merged and "null" in merged)
        )

    def _is_transient_upload_unavailable(self, payload: Any, err: Optional[Exception] = None) -> bool:
        text_parts = []
        if err is not None:
            text_parts.append(str(err))

        if isinstance(payload, dict):
            code = str(payload.get("code") or payload.get("res_code") or "").strip()
            if code:
                text_parts.append(code)
            text_parts.extend([
                str(payload.get("msg") or ""),
                str(payload.get("message") or ""),
                str(payload.get("res_message") or ""),
            ])
            data = payload.get("data")
            if isinstance(data, dict):
                data_code = str(data.get("code") or data.get("res_code") or "").strip()
                if data_code:
                    text_parts.append(data_code)
                text_parts.extend([
                    str(data.get("msg") or ""),
                    str(data.get("message") or ""),
                    str(data.get("res_message") or ""),
                ])

        merged = " | ".join([x for x in text_parts if x]).lower()
        return ("-1" in merged) and ("暂时不可用" in merged or "temporarily unavailable" in merged)

    async def _ensure_upload_session_key(self) -> Optional[str]:
        if self.session_key:
            self.client.__dict__["session_key"] = self.session_key
            return self.session_key

        cached = self.client.__dict__.get("session_key")
        if isinstance(cached, str) and cached.strip():
            self.session_key = cached.strip()
            return self.session_key

        cookie_str = (getattr(self.client, "cookies_str", "") or "").strip()
        match = re.search(r"\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?:_family)?)\b", cookie_str, re.IGNORECASE)
        if match:
            sk = match.group(1)
            self.session_key = sk
            self.client.__dict__["session_key"] = sk
            logger.info("【P189Client】已从 cookies 自举 sessionKey")
            return sk

        for fetcher_name in ("user_info_brief", "user_logined_infos_portal", "user_logined_infos_v2", "user_logined_infos"):
            fetcher = getattr(self.client, fetcher_name, None)
            if not fetcher:
                continue
            try:
                resp = await fetcher(async_=True)
                sk = self._pick_session_key(resp)
                if sk:
                    self.session_key = sk
                    self.client.__dict__["session_key"] = sk
                    logger.info(f"【P189Client】已通过 {fetcher_name} 自举 sessionKey")
                    return sk
            except Exception as e:
                logger.warning(f"【P189Client】通过 {fetcher_name} 获取 sessionKey 失败: {type(e).__name__}: {e}")

        if self.is_logged_in:
            relog_ok = await self.login(force=True)
            if relog_ok and self.session_key:
                self.client.__dict__["session_key"] = self.session_key
                logger.info("【P189Client】通过强制重登录拿到 sessionKey")
                return self.session_key

        logger.warning("【P189Client】sessionKey 不可用，上传接口可能失败")
        return None

    async def _try_reauth_with_session_key(self) -> Optional[str]:
        session_key = (self.session_key or "").strip()
        if not session_key:
            try:
                session_key = str(getattr(self.client, "session_key", "") or "").strip()
            except Exception:
                session_key = ""
        if not session_key:
            return None

        try:
            resp = await P189Client.login_with_session_key(session_key, async_=True)
            cookies = ""
            if isinstance(resp, dict):
                cookies = P189Client(cookies=resp.get("cookies") or {}).cookies_str
            elif isinstance(resp, str):
                cookies = resp.strip()
            if cookies:
                logger.info("【P189Client】通过 sessionKey 刷新会话成功")
                return cookies
        except Exception as e:
            logger.warning(f"【P189Client】login_with_session_key 失败: {type(e).__name__}: {e}")
        return None

    async def _reauth_and_reinit_client(self, reason: str = "session_invalid") -> bool:
        old_cookies = (getattr(self.client, "cookies_str", "") or "").strip()
        logger.warning(f"【P189Client】开始重认证 reason={reason}")

        candidates: List[str] = []
        by_session_key = await self._try_reauth_with_session_key()
        if by_session_key:
            candidates.append(by_session_key)
        if old_cookies:
            candidates.append(old_cookies)

        seen = set()
        for cookies in candidates:
            cookies = (cookies or "").strip()
            if not cookies or cookies in seen:
                continue
            seen.add(cookies)

            self.client = P189Client(cookies=cookies)
            self._save_cookie_store()
            session_key = await self._ensure_upload_session_key()
            if session_key:
                await self._refresh_upload_crypto_context(reason=f"reauth_{reason}", refresh_session_key=False)
                logger.info("【P189Client】重认证成功")
                return True

        relog_ok = await self.login(force=True)
        if relog_ok:
            session_key = await self._ensure_upload_session_key()
            if session_key:
                await self._refresh_upload_crypto_context(reason=f"relogin_{reason}", refresh_session_key=False)
                logger.info("【P189Client】强制重登录后重认证成功")
                return True

        logger.error(f"【P189Client】重认证失败 reason={reason}")
        return False

    async def _protected_client_call(self, attr_name: str, *args, **kwargs) -> Any:
        """
        带自动重认证机制的 API 调用封装
        """
        method = getattr(self.client, attr_name, None)
        if not method:
            raise AttributeError(f"P189Client has no method {attr_name}")

        res = None
        err = None
        try:
            import asyncio
            # 优先判断是否为协程函数
            if asyncio.iscoroutinefunction(method):
                res = await method(*args, **kwargs)
            else:
                # 尝试调用
                res_or_coro = method(*args, **kwargs)
                if asyncio.iscoroutine(res_or_coro):
                    res = await res_or_coro
                else:
                    res = res_or_coro
        except Exception as e:
            err = e

        if self._is_session_invalid_payload(res, err):
            logger.warning(f"【P189Client】调用 {attr_name} 时检测到会话失效，尝试强制重认证...")
            if await self._reauth_and_reinit_client(reason=f"api_{attr_name}_invalid"):
                # 重置方法引用，因为 self.client 已被重新初始化
                method = getattr(self.client, attr_name, None)
                return await method(*args, **kwargs)

        if err:
            raise err
        return res

    async def _reauth_for_upload(self) -> bool:
        return await self._reauth_and_reinit_client(reason="upload_session_invalid")

    async def share_check(self, share_code: str, access_code: str) -> Optional[dict]:
        """
        校验分享链接并获取信息
        """
        try:
            logger.info(f"【P189Client】正在获取分享信息: shareCode={share_code}")
            code = (share_code or "").strip()
            if access_code:
                check = await self.client.share_check_access_token(
                    {"shareCode": code, "accessCode": access_code},
                    async_=True,
                )
                check_response(check)

            res = await self.client.share_info_by_code({"shareCode": code}, async_=True)
            check_response(res)
            logger.info(f"【P189Client】分享详情获取成功: {res.get('shareId')}")
            return res
        except Exception as e:
            logger.error(f"【P189Client】分享校验失败: {e}")
            return None

    async def share_list_all(self, share_id: str, access_code: str, share_code: str = "") -> List[dict]:
        """
        获取分享内的所有文件列表（递归平铺，支持分页）
        """

        def _collect_entries(resp: Any) -> tuple[List[dict], List[dict]]:
            files: List[dict] = []
            folders: List[dict] = []
            if not isinstance(resp, dict):
                return files, folders

            if isinstance(resp.get("fileListAO"), dict):
                ao = resp.get("fileListAO") or {}
                files.extend(ao.get("fileList") or [])
                folders.extend(ao.get("folderList") or [])
            elif isinstance(resp.get("fileList"), list):
                files.extend(resp.get("fileList") or [])
                folders.extend(resp.get("folderList") or [])

            data = resp.get("data")
            if isinstance(data, dict):
                if isinstance(data.get("fileListAO"), dict):
                    ao = data.get("fileListAO") or {}
                    files.extend(ao.get("fileList") or [])
                    folders.extend(ao.get("folderList") or [])
                elif isinstance(data.get("fileList"), list):
                    files.extend(data.get("fileList") or [])
                    folders.extend(data.get("folderList") or [])

            return files, folders

        async def _list_dir_open(file_id: Optional[str], page_num: int, page_size: int) -> tuple[List[dict], List[dict]]:
            payload = {"shareId": share_id, "pageNum": page_num, "pageSize": page_size}
            if file_id:
                payload["fileId"] = file_id
            if access_code:
                payload["accessCode"] = access_code
            resp = await self.client.share_fs_list(payload, async_=True)
            check_response(resp)
            return _collect_entries(resp)

        async def _list_dir_portal(file_id: Optional[str], page_num: int, page_size: int) -> tuple[List[dict], List[dict]]:
            if not share_code:
                return [], []
            payload = {"shortCode": share_code, "pageNum": page_num, "pageSize": page_size}
            if file_id:
                payload["fileId"] = file_id
            if access_code:
                payload["accessCode"] = access_code
            resp = await self.client.share_fs_list_portal(payload, async_=True)
            check_response(resp)
            return _collect_entries(resp)

        async def _list_dir_all_pages(file_id: Optional[str]) -> tuple[List[dict], List[dict]]:
            page_num = 1
            page_size = 100
            all_files: List[dict] = []
            all_folders: List[dict] = []

            while True:
                files: List[dict] = []
                folders: List[dict] = []
                open_err = None

                try:
                    files, folders = await _list_dir_open(file_id, page_num, page_size)
                except Exception as e:
                    open_err = e
                    try:
                        files, folders = await _list_dir_portal(file_id, page_num, page_size)
                    except Exception as pe:
                        logger.warning(
                            f"【P189Client】目录列表分页失败 fileId={file_id} page={page_num}: open={open_err} portal={pe}"
                        )
                        break

                all_files.extend(files)
                all_folders.extend(folders)

                if len(files) + len(folders) < page_size:
                    break
                page_num += 1

            return all_files, all_folders

        try:
            logger.info(f"【P189Client】正在列出分享文件，shareId={share_id}")

            root_file_id = None
            try:
                if share_code:
                    info_resp = await self.client.share_info_by_code({"shareCode": share_code}, async_=True)
                    check_response(info_resp)
                    root_file_id = info_resp.get("fileId") or info_resp.get("shareDirFileId")
            except Exception:
                root_file_id = None

            all_items: List[dict] = []
            seen_ids = set()
            queue = [(str(root_file_id), "")] if root_file_id else [(None, "")]

            while queue:
                curr_id, curr_rel_path = queue.pop(0)
                files, folders = await _list_dir_all_pages(curr_id)

                for item in files:
                    if not isinstance(item, dict):
                        continue
                    iid = str(item.get("id") or item.get("fileId") or "")
                    if iid and iid in seen_ids:
                        continue
                    if iid:
                        seen_ids.add(iid)
                    copied = dict(item)
                    copied["_rel_dir"] = curr_rel_path
                    all_items.append(copied)

                for folder in folders:
                    if not isinstance(folder, dict):
                        continue
                    fid = str(folder.get("id") or folder.get("fileId") or folder.get("folderId") or "")
                    if fid and fid in seen_ids:
                        continue

                    folder_name = str(folder.get("name") or folder.get("folderName") or "").strip()
                    next_rel = f"{curr_rel_path}/{folder_name}".strip("/") if folder_name else curr_rel_path

                    copied = dict(folder)
                    copied["_rel_dir"] = curr_rel_path
                    all_items.append(copied)

                    if fid:
                        seen_ids.add(fid)
                        queue.append((fid, next_rel))

            logger.info(f"【P189Client】列表拉取完成，共发现 {len(all_items)} 个项目。")
            return all_items
        except Exception as e:
            logger.error(f"【P189Client】获取分享列表异常: {e}")
            return []

    async def share_save_to_local(
        self,
        share_id: str,
        share_code: str,
        file_id: str,
        access_code: str,
        target_folder_id: str,
        expected_name: str = "",
        is_folder: bool = False,
    ) -> Optional[str]:
        """
        将分享文件或目录转存到指定目录，返回新资源 ID
        """
        try:
            logger.info(
                f"【P189Client】正在发起转存任务: fileId={file_id} is_folder={is_folder} -> folderId={target_folder_id}"
            )
            task_name = (expected_name or "").strip() or "."
            payload = {
                "type": "SHARE_SAVE",
                "shareId": str(share_id),
                "shareCode": str(share_code),
                "targetFolderId": str(target_folder_id) if target_folder_id else "-11",
                "taskInfos": [{
                    "fileId": str(file_id),
                    "fileName": task_name,
                    "isFolder": 1 if is_folder else 0,
                }]
            }

            task_id = None
            final_check = None
            for create_try in range(1, 4):
                try:
                    res = await self._protected_client_call("fs_batch", payload, async_=True)
                    check_response(res)
                    if isinstance(res, dict):
                        res_code = str(res.get("res_code", "")).strip()
                        if res_code and res_code not in ("0", "SUCCESS"):
                            # 如果受保护调用后依然返回失效（理论上极少发生），则记录后退出
                            if self._is_session_invalid_payload(res):
                                logger.error(f"【P189Client】重认证后 API 依然失效: {res}")
                                return None
                            raise RuntimeError(f"share_save create failed: {res}")
                    if isinstance(res, dict):
                        task_id = res.get("taskId") or res.get("taskID") or res.get("id")
                        if not task_id and isinstance(res.get("data"), dict):
                            task_id = (res.get("data") or {}).get("data", {}).get("taskId") or (res.get("data") or {}).get("taskId")
                    if task_id:
                        break
                    logger.error(f"【P189Client】转存创建成功但未返回 taskId: {res}")
                    return None
                except Exception as ce:
                    msg = str(ce)
                    if "ShareSaveTaskIsAlreadyExist" in msg and create_try < 3:
                        await asyncio.sleep(0.8 * create_try)
                        continue
                    logger.error(f"【P189Client】转存操作失败: {ce}")
                    return None

            if not task_id:
                logger.error(f"【P189Client】转存创建失败且未拿到 taskId fileId={file_id}")
                return None

            for _ in range(20):
                check = await self.client.fs_batch_check({"taskId": task_id, "type": "SHARE_SAVE"}, async_=True)
                check_response(check)
                final_check = check

                status_raw = check.get("taskStatus") if isinstance(check, dict) else None
                try:
                    status = int(status_raw) if status_raw is not None else None
                except Exception:
                    status = status_raw

                if status in (-1, 2):
                    logger.error(f"【P189Client】转存任务失败 taskId={task_id}, check={check}")
                    return None

                sub_count = int((check or {}).get("subTaskCount") or 0) if isinstance(check, dict) else 0
                finished = (
                    int((check or {}).get("successedCount") or 0)
                    + int((check or {}).get("failedCount") or 0)
                    + int((check or {}).get("skipCount") or 0)
                ) if isinstance(check, dict) else 0

                if status == 4 or (sub_count > 0 and finished >= sub_count):
                    break

                await asyncio.sleep(0.6)

            expected_name_norm = (expected_name or "").strip()
            for _ in range(6):
                page_num = 1
                page_size = 100
                while True:
                    listing = await self._protected_client_call(
                        "fs_list",
                        {"folderId": str(target_folder_id), "pageNum": page_num, "pageSize": page_size},
                        async_=True,
                    )
                    check_response(listing)

                    files = listing.get("fileListAO", {}).get("fileList", []) or listing.get("fileList", [])
                    folders = listing.get("fileListAO", {}).get("folderList", []) or listing.get("folderList", [])

                    if is_folder:
                        if expected_name_norm:
                            for fd in folders:
                                fname = str(fd.get("name") or fd.get("folderName") or "").strip()
                                if fname == expected_name_norm:
                                    return str(fd.get("id") or fd.get("folderId") or fd.get("fileId") or "")
                        for fd in folders:
                            old_id = str(fd.get("oldFileId") or "")
                            new_id = str(fd.get("id") or fd.get("folderId") or fd.get("fileId") or "")
                            if old_id == str(file_id) or new_id == str(file_id):
                                return new_id
                    else:
                        if expected_name_norm:
                            for f in files:
                                fname = str(f.get("name") or f.get("fileName") or "").strip()
                                if fname == expected_name_norm:
                                    return str(f.get("id") or f.get("fileId") or "")
                        for f in files:
                            old_id = str(f.get("oldFileId") or "")
                            new_file_id = str(f.get("fileId") or f.get("id") or "")
                            if old_id == str(file_id) or new_file_id == str(file_id):
                                return new_file_id

                    if len(files) + len(folders) < page_size:
                        break
                    page_num += 1

                await asyncio.sleep(0.5)

            logger.error(
                f"【P189Client】转存任务完成但目标目录未识别到结果: folderId={target_folder_id}, is_folder={is_folder}, taskCheck={final_check}"
            )
            return None
        except Exception as e:
            logger.error(f"【P189Client】转存操作失败: {e}")
            return None

    async def fs_read_content(self, file_id: str) -> Optional[str]:
        """
        读取文件文本内容 (CAS 文件都是 Base64)
        """
        try:
            logger.info(f"【P189Client】正在读取文件内容: fileId={file_id}")
            url = await self.get_download_url(file_id)
            if not url:
                return None
            
            import httpx
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                res = await client.get(url)
                res.raise_for_status()
                logger.info(f"【P189Client】文件内容读取成功，长度: {len(res.text)}")
                return res.text
        except Exception as e:
            logger.error(f"【P189Client】文件内容读取失败: {e}")
            return None

    async def fs_mkdir(self, parent_id: str, name: str) -> Optional[str]:
        """
        创建目录
        """
        try:
            exist_id = await self.fs_get_path_id_child(parent_id, name)
            if exist_id:
                return exist_id
                
            logger.info(f"【P189Client】正在创建云盘目录: {name} (parent={parent_id})")
            payload = {
                "parentFolderId": str(parent_id),
                "folderName": name
            }
            res = await self._protected_client_call("fs_mkdir", payload)
            check_response(res)
            return str(res.get("id") or res.get("folderId"))
        except Exception as e:
            logger.error(f"【P189Client】创建目录失败: {e}")
            return None

    async def fs_get_path_id_child(self, parent_id: str, name: str) -> Optional[str]:
        """
        获取子目录 ID
        """
        try:
            listing = self.client.fs_list(parent_id)
            folders = listing.get('fileListAO', {}).get('folderList', []) or listing.get('folderList', [])
            for folder in folders:
                if folder.get("name") == name or folder.get("folderName") == name:
                    return str(folder.get("id") or folder.get("folderId"))
        except: pass
        return None

    async def fs_get_path_id(self, path: str) -> Optional[str]:
        """
        通过路径获取目录 ID
        """
        if not path or path == "/":
            return "-11" 
        
        parts = [p for p in path.split("/") if p]
        curr_id = "-11"
        for part in parts:
            found_id = await self.fs_get_path_id_child(curr_id, part)
            if found_id:
                curr_id = str(found_id)
            else:
                curr_id = await self.fs_mkdir(curr_id, part)
                if not curr_id:
                    return None
        return str(curr_id)

    async def fs_delete(self, file_id: str, is_folder: bool = False) -> bool:
        """
        删除资源并等待任务完成
        """
        try:
            logger.info(f"【P189Client】正在执行删除任务: ID={file_id} is_folder={is_folder}")
            payload = {
                "type": "DELETE",
                "taskInfos": [{
                    "fileId": str(file_id),
                    "isFolder": 1 if is_folder else 0
                }]
            }
            res = await self.client.fs_batch(payload, async_=True)
            check_response(res)

            task_id = None
            if isinstance(res, dict):
                task_id = res.get("taskId") or res.get("taskID") or res.get("id")
                if not task_id and isinstance(res.get("data"), dict):
                    task_id = (res.get("data") or {}).get("taskId") or (res.get("data") or {}).get("taskID")

            if not task_id:
                logger.warning(f"【P189Client】删除任务未返回 taskId，按提交成功处理: {res}")
                return True

            final_check = None
            for _ in range(20):
                check = await self.client.fs_batch_check({"taskId": task_id, "type": "DELETE"}, async_=True)
                check_response(check)
                final_check = check

                status_raw = check.get("taskStatus") if isinstance(check, dict) else None
                try:
                    status = int(status_raw) if status_raw is not None else None
                except Exception:
                    status = status_raw

                failed = int((check or {}).get("failedCount") or 0) if isinstance(check, dict) else 0
                sub_count = int((check or {}).get("subTaskCount") or 0) if isinstance(check, dict) else 0
                finished = (
                    int((check or {}).get("successedCount") or 0)
                    + int((check or {}).get("failedCount") or 0)
                    + int((check or {}).get("skipCount") or 0)
                ) if isinstance(check, dict) else 0

                if status in (-1, 2):
                    logger.error(f"【P189Client】删除任务失败 taskId={task_id}, check={check}")
                    return False

                if status == 4 or (sub_count > 0 and finished >= sub_count):
                    if failed > 0:
                        logger.error(f"【P189Client】删除任务完成但存在失败 taskId={task_id}, check={check}")
                        return False
                    logger.info(f"【P189Client】删除任务完成 taskId={task_id}")
                    return True

                await asyncio.sleep(0.5)

            logger.error(f"【P189Client】删除任务等待超时 taskId={task_id}, check={final_check}")
            return False
        except Exception as e:
            logger.error(f"【P189Client】删除失败: {e}")
            return False

    async def get_download_url(self, file_id: str) -> Optional[str]:
        """
        获取 302 下载链接（兼容 download_url_info / download_url 多种返回）
        """
        fid_text = str(file_id or "").strip()
        if not fid_text:
            return None

        def _extract_url(resp: Any) -> Optional[str]:
            if isinstance(resp, str) and resp.strip():
                return resp.strip()
            if not isinstance(resp, dict):
                return None

            # 优先取业务载荷中的真实下载地址（如 normal.url）
            preferred_nodes: List[dict] = []
            for key in ("normal", "data", "result", "res", "body"):
                val = resp.get(key)
                if isinstance(val, dict):
                    preferred_nodes.append(val)
            preferred_nodes.append(resp)

            for node in preferred_nodes:
                for key in ("fileDownloadUrl", "downloadUrl", "url"):
                    url = node.get(key)
                    if isinstance(url, str) and url.strip():
                        return url.strip()
            return None

        async def _resolve_location(url: str) -> Optional[str]:
            raw_url = str(url or "").strip()
            if not raw_url:
                return None
            try:
                import httpx
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
                }
                cookies = self.client.cookies or None
                async with httpx.AsyncClient(timeout=30, follow_redirects=False, headers=headers, cookies=cookies) as client:
                    resp = await client.get(raw_url)
                    location = resp.headers.get("location") or resp.headers.get("Location")
                    if isinstance(location, str) and location.strip():
                        return location.strip()
                    logger.warning(
                        f"【P189Client】302解引用未返回Location fileId={fid_text} status={resp.status_code} url={raw_url[:120]}"
                    )
                    return None
            except Exception as e:
                logger.warning(f"【P189Client】解析302 Location失败 fileId={fid_text}: {e}")
                return None

        payloads: List[Union[dict, str, int]] = [{"fileId": fid_text}]
        try:
            payloads.append({"fileId": int(fid_text)})
        except Exception:
            pass
        payloads.append(fid_text)

        attempts: List[Tuple[str, Union[dict, str, int]]] = []
        for p in payloads:
            attempts.append(("download_url_info", p))
        for p in payloads:
            attempts.append(("download_url", p))
        for p in payloads:
            attempts.append(("download_url_video_portal", p))

        for method_name, payload in attempts:
            method = getattr(self.client, method_name, None)
            if not method:
                continue
            try:
                resp = method(payload)
                if isinstance(resp, dict):
                    try:
                        check_response(resp)
                    except Exception:
                        pass
                url = _extract_url(resp)
                if url:
                    final_url = await _resolve_location(url)
                    if final_url:
                        logger.info(f"【P189Client】获取下载链接成功 method={method_name} fileId={fid_text}")
                        return final_url
                    logger.warning(f"【P189Client】下载链接解302失败 method={method_name} fileId={fid_text} first_url={url[:120]}")
                logger.warning(f"【P189Client】下载接口无直链字段 method={method_name} fileId={fid_text}, resp={resp}")
            except Exception as e:
                logger.warning(f"【P189Client】下载接口调用失败 method={method_name} fileId={fid_text}: {e}")

        logger.error(f"【P189Client】获取下载链接失败，所有路径均未返回直链: fileId={fid_text}")
        return None

    async def rapid_upload(self, parent_id: str, filename: str, size: int, md5: str, slice_md5: str) -> bool:
        """
        执行秒传三段式流程：initMultiUpload -> checkTransSecond -> commitMultiUploadFile
        """
        try:
            file_md5 = str(md5 or "").strip().upper()
            file_slice_md5 = str(slice_md5 or "").strip().upper()
            safe_name = (str(filename or "").split("/")[-1].strip() or "recovered_from_cas.bin")
            file_size = int(size)

            logger.info(
                f"【P189Client】执行秒传校验[P189PATCH-20260421]: {safe_name} "
                f"(size={file_size}, md5={file_md5}, sliceMd5={file_slice_md5})"
            )

            logger.info(f"【P189Client】当前 cookies 长度: {len((self.client.cookies_str or '').strip())}")

            session_key = await self._ensure_upload_session_key()
            logger.info(f"【P189Client】upload 前 session_key={self._session_key_brief(session_key)}")
            if not session_key:
                logger.error("【P189Client】秒传失败：sessionKey 不可用")
                return False

            pkid = await self._peek_upload_pkid()
            if not pkid:
                logger.error("【P189Client】秒传失败：upload pkid 不可用")
                return False

            def part_size(raw_size: int) -> int:
                unit = 10485760
                if raw_size > unit * 2 * 999:
                    return max((raw_size + unit * 1999 - 1) // (unit * 1999), 5) * unit
                if raw_size > unit * 999:
                    return unit * 2
                return unit

            slice_size = part_size(file_size)
            init_payload = {
                "parentFolderId": str(parent_id),
                "fileName": quote(safe_name, safe=""),
                "fileSize": int(file_size),
                "sliceSize": int(slice_size),
                "lazyCheck": "1",
            }

            logger.info(
                f"【P189Client】upload_init prepare file={safe_name} parent={parent_id} "
                f"session_key={self._session_key_brief(session_key)} pkid={pkid}"
            )

            async def _call_upload_stage(api: str, payload: dict, stage: str, max_attempts: int = 3) -> dict:
                retry_delays = (0.8, 1.6, 2.5)
                last_error = None

                for attempt in range(1, max_attempts + 1):
                    try:
                        resp = await self._upload_request_with_pubkey(api, payload)
                        logger.info(f"【P189Client】{stage} file={safe_name} attempt={attempt} resp={resp}")

                        try:
                            check_response(resp)
                        except Exception as e:
                            last_error = e
                            if self._is_session_invalid_payload(resp, e):
                                logger.warning(f"【P189Client】{stage} session invalid attempt={attempt}, reauth and retry")
                                reauth_ok = await self._reauth_for_upload()
                                if reauth_ok and attempt < max_attempts:
                                    await self._refresh_upload_crypto_context(
                                        reason=f"upload_{stage}_reauth",
                                        refresh_session_key=True,
                                    )
                                    continue
                                return {
                                    "ok": False,
                                    "message": f"{stage}_session_invalid",
                                    "resp": resp,
                                }

                            transient = self._is_transient_upload_unavailable(resp, e)
                            if transient and attempt < max_attempts:
                                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                                logger.warning(
                                    f"【P189Client】{stage} transient_unavailable attempt={attempt} "
                                    f"retry_after={delay}s err={type(e).__name__}: {e}"
                                )
                                await asyncio.sleep(delay)
                                continue
                            raise

                        if self._is_transient_upload_unavailable(resp):
                            if attempt < max_attempts:
                                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                                logger.warning(
                                    f"【P189Client】{stage} transient_unavailable attempt={attempt} retry_after={delay}s"
                                )
                                await asyncio.sleep(delay)
                                continue
                            return {
                                "ok": False,
                                "message": f"{stage}_temporary_unavailable",
                                "resp": resp,
                            }

                        if self._is_session_invalid_payload(resp):
                            logger.warning(
                                f"【P189Client】{stage} payload indicates session invalid attempt={attempt}, reauth and retry"
                            )
                            reauth_ok = await self._reauth_for_upload()
                            if reauth_ok and attempt < max_attempts:
                                await self._refresh_upload_crypto_context(
                                    reason=f"upload_{stage}_payload_reauth",
                                    refresh_session_key=True,
                                )
                                continue
                            return {
                                "ok": False,
                                "message": f"{stage}_session_invalid_payload",
                                "resp": resp,
                            }

                        return {"ok": True, "resp": resp}
                    except Exception as e:
                        last_error = e
                        if self._is_session_invalid_payload(None, e):
                            logger.warning(
                                f"【P189Client】{stage} session exception attempt={attempt}, reauth and retry "
                                f"err={type(e).__name__}: {e}"
                            )
                            reauth_ok = await self._reauth_for_upload()
                            if reauth_ok and attempt < max_attempts:
                                await self._refresh_upload_crypto_context(
                                    reason=f"upload_{stage}_exception_reauth",
                                    refresh_session_key=True,
                                )
                                continue
                            return {"ok": False, "message": f"{stage}_session_invalid_exception: {e}", "resp": None}

                        if self._is_transient_upload_unavailable(None, e) and attempt < max_attempts:
                            delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                            logger.warning(
                                f"【P189Client】{stage} transient_exception attempt={attempt} "
                                f"retry_after={delay}s err={type(e).__name__}: {e}"
                            )
                            await asyncio.sleep(delay)
                            continue
                        return {"ok": False, "message": f"{type(e).__name__}: {e}", "resp": None}

                return {
                    "ok": False,
                    "message": f"{type(last_error).__name__}: {last_error}" if last_error else f"{stage}_unknown_error",
                    "resp": None,
                }

            init_stage = await _call_upload_stage("initMultiUpload", init_payload, "upload_init")
            if not init_stage.get("ok"):
                logger.error(f"【P189Client】秒传 init 失败: {init_stage}")
                return False

            init_resp = init_stage.get("resp") or {}
            init_data = init_resp.get("data") if isinstance(init_resp, dict) else None
            if not isinstance(init_data, dict):
                init_data = init_resp if isinstance(init_resp, dict) else {}

            upload_file_id = init_data.get("uploadFileId")
            if not upload_file_id:
                logger.error(f"【P189Client】秒传 init 阶段未返回 uploadFileId: {init_resp}")
                return False

            check_stage = await _call_upload_stage(
                "checkTransSecond",
                {
                    "uploadFileId": upload_file_id,
                    "fileMd5": file_md5,
                    "sliceMd5": file_slice_md5,
                },
                "second_check",
            )
            if not check_stage.get("ok"):
                logger.error(f"【P189Client】秒传 second_check 失败: {check_stage}")
                return False

            check_resp = check_stage.get("resp") or {}
            check_data = check_resp.get("data") if isinstance(check_resp, dict) else None
            if not isinstance(check_data, dict):
                check_data = check_resp if isinstance(check_resp, dict) else {}

            if check_data.get("fileDataExists") not in (1, True, "1"):
                logger.error(f"【P189Client】秒传二检未命中(fileDataExists!=1): {check_resp}")
                return False

            commit_stage = await _call_upload_stage(
                "commitMultiUploadFile",
                {
                    "uploadFileId": upload_file_id,
                    "fileMd5": file_md5,
                    "sliceMd5": file_slice_md5,
                    "lazyCheck": 1,
                    "opertype": "3",
                },
                "commit",
            )
            if not commit_stage.get("ok"):
                logger.error(f"【P189Client】秒传 commit 失败: {commit_stage}")
                return False

            commit_resp = commit_stage.get("resp") or {}
            commit_code = str(commit_resp.get("code") if isinstance(commit_resp, dict) else "")
            if commit_code and commit_code not in ("SUCCESS", "0"):
                logger.error(f"【P189Client】秒传提交失败 code={commit_code}: {commit_resp}")
                return False

            logger.info(f"【P189Client】秒传成功: {safe_name}")
            return True
        except Exception as e:
            logger.error(f"【P189Client】秒传校验过程异常: {e}")
            return False

    async def fs_empty_recycle(self) -> bool:
        """
        清空回收站并等待任务完成
        """
        try:
            logger.info("【P189Client】正在执行清空回收站任务...")
            payload = {
                "type": "EMPTY_RECYCLE",
                "taskInfos": []
            }
            res = await self._protected_client_call("fs_batch", payload, async_=True)
            check_response(res)

            task_id = None
            if isinstance(res, dict):
                task_id = res.get("taskId") or res.get("taskID") or res.get("id")
                if not task_id and isinstance(res.get("data"), dict):
                    task_id = (res.get("data") or {}).get("taskId") or (res.get("data") or {}).get("taskID")

            if not task_id:
                logger.warning(f"【P189Client】清空回收站任务未返回 taskId，按提交成功处理: {res}")
                return True

            final_check = None
            for _ in range(40):  # 增加尝试次数，清空可能较慢
                check = await self._protected_client_call("fs_batch_check", {"taskId": task_id, "type": "EMPTY_RECYCLE"}, async_=True)
                check_response(check)
                final_check = check

                status_raw = check.get("taskStatus") if isinstance(check, dict) else None
                try:
                    status = int(status_raw) if status_raw is not None else None
                except Exception:
                    status = status_raw

                failed = int((check or {}).get("failedCount") or 0) if isinstance(check, dict) else 0
                sub_count = int((check or {}).get("subTaskCount") or 0) if isinstance(check, dict) else 0
                finished = (
                    int((check or {}).get("successedCount") or 0)
                    + int((check or {}).get("failedCount") or 0)
                    + int((check or {}).get("skipCount") or 0)
                ) if isinstance(check, dict) else 0

                if status in (-1, 2):
                    logger.error(f"【P189Client】清空回收站任务失败 taskId={task_id}, check={check}")
                    return False

                if status == 4 or (sub_count > 0 and finished >= sub_count):
                    if failed > 0:
                        logger.error(f"【P189Client】清空回收站任务完成但存在失败 taskId={task_id}, check={check}")
                        return False
                    logger.info(f"【P189Client】清空回收站任务完成 taskId={task_id}")
                    return True

                await asyncio.sleep(0.5)

            logger.error(f"【P189Client】清空回收站任务等待超时 taskId={task_id}, check={final_check}")
            return False
        except Exception as e:
            logger.error(f"【P189Client】清空回收站失败: {e}")
            return False
